//! Low-level binary reader for STDF fields.

use std::io::{self, Read};

/// Endian-aware STDF binary reader.
pub struct StdfReader {
    /// True = little-endian, false = big-endian.
    pub little_endian: bool,
}

impl StdfReader {
    pub fn new() -> Self {
        Self {
            little_endian: true,
        }
    }

    /// Read unsigned 1-byte integer.
    #[inline]
    pub fn read_u1<R: Read>(&self, r: &mut R) -> io::Result<u8> {
        let mut buf = [0u8; 1];
        r.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    /// Read unsigned 2-byte integer (endian-aware).
    #[inline]
    pub fn read_u2<R: Read>(&self, r: &mut R) -> io::Result<u16> {
        let mut buf = [0u8; 2];
        r.read_exact(&mut buf)?;
        Ok(if self.little_endian {
            u16::from_le_bytes(buf)
        } else {
            u16::from_be_bytes(buf)
        })
    }

    /// Read unsigned 4-byte integer (endian-aware).
    #[inline]
    pub fn read_u4<R: Read>(&self, r: &mut R) -> io::Result<u32> {
        let mut buf = [0u8; 4];
        r.read_exact(&mut buf)?;
        Ok(if self.little_endian {
            u32::from_le_bytes(buf)
        } else {
            u32::from_be_bytes(buf)
        })
    }

    /// Read signed 1-byte integer.
    #[inline]
    pub fn read_i1<R: Read>(&self, r: &mut R) -> io::Result<i8> {
        let mut buf = [0u8; 1];
        r.read_exact(&mut buf)?;
        Ok(buf[0] as i8)
    }

    /// Read signed 2-byte integer (endian-aware).
    #[inline]
    pub fn read_i2<R: Read>(&self, r: &mut R) -> io::Result<i16> {
        let mut buf = [0u8; 2];
        r.read_exact(&mut buf)?;
        Ok(if self.little_endian {
            i16::from_le_bytes(buf)
        } else {
            i16::from_be_bytes(buf)
        })
    }

    /// Read 4-byte float (endian-aware).
    #[inline]
    pub fn read_r4<R: Read>(&self, r: &mut R) -> io::Result<f32> {
        let mut buf = [0u8; 4];
        r.read_exact(&mut buf)?;
        Ok(if self.little_endian {
            f32::from_le_bytes(buf)
        } else {
            f32::from_be_bytes(buf)
        })
    }

    /// Read character string (length-prefixed, 1-byte length).
    pub fn read_cn<R: Read>(&self, r: &mut R) -> io::Result<String> {
        let len = self.read_u1(r)? as usize;
        if len == 0 {
            return Ok(String::new());
        }
        let mut buf = vec![0u8; len];
        r.read_exact(&mut buf)?;
        Ok(String::from_utf8_lossy(&buf).to_string())
    }

    /// Read record header: (rec_len, rec_typ, rec_sub).
    pub fn read_header<R: Read>(&self, r: &mut R) -> io::Result<(u16, u8, u8)> {
        let rec_len = self.read_u2(r)?;
        let rec_typ = self.read_u1(r)?;
        let rec_sub = self.read_u1(r)?;
        Ok((rec_len, rec_typ, rec_sub))
    }

    /// Skip `n` bytes.
    pub fn skip<R: Read>(&self, r: &mut R, n: usize) -> io::Result<()> {
        let mut buf = vec![0u8; n];
        r.read_exact(&mut buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_read_u1() {
        let reader = StdfReader::new();
        let mut cursor = Cursor::new(vec![0x42]);
        assert_eq!(reader.read_u1(&mut cursor).unwrap(), 0x42);
    }

    #[test]
    fn test_read_u2_le() {
        let reader = StdfReader::new();
        let mut cursor = Cursor::new(vec![0x01, 0x02]);
        assert_eq!(reader.read_u2(&mut cursor).unwrap(), 0x0201);
    }

    #[test]
    fn test_read_u2_be() {
        let reader = StdfReader { little_endian: false };
        let mut cursor = Cursor::new(vec![0x01, 0x02]);
        assert_eq!(reader.read_u2(&mut cursor).unwrap(), 0x0102);
    }

    #[test]
    fn test_read_u4_le() {
        let reader = StdfReader::new();
        // 210000 = 0x00033450 in LE: [0x50, 0x34, 0x03, 0x00]
        let mut cursor = Cursor::new(vec![0x50, 0x34, 0x03, 0x00]);
        assert_eq!(reader.read_u4(&mut cursor).unwrap(), 210000);
    }

    #[test]
    fn test_read_cn() {
        let reader = StdfReader::new();
        let mut cursor = Cursor::new(vec![0x05, b'H', b'e', b'l', b'l', b'o']);
        assert_eq!(reader.read_cn(&mut cursor).unwrap(), "Hello");
    }

    #[test]
    fn test_read_cn_empty() {
        let reader = StdfReader::new();
        let mut cursor = Cursor::new(vec![0x00]);
        assert_eq!(reader.read_cn(&mut cursor).unwrap(), "");
    }

    #[test]
    fn test_read_r4_le() {
        let reader = StdfReader::new();
        let val: f32 = 3.14;
        let bytes = val.to_le_bytes();
        let mut cursor = Cursor::new(bytes.to_vec());
        let result = reader.read_r4(&mut cursor).unwrap();
        assert!((result - 3.14).abs() < 0.001);
    }
}
