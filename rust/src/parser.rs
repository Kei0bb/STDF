//! STDF record parser — mirrors Python STDFParser.

use std::fs::File;
use std::io::{self, BufReader, Cursor, Read};
use std::path::Path;

use flate2::read::GzDecoder;

use crate::reader::StdfReader;
use crate::types::*;

/// Internal parser state.
struct ParserState {
    data: StdfData,
    reader: StdfReader,
    part_counter: i64,
    current_wafer: String,
}

impl ParserState {
    fn new() -> Self {
        Self {
            data: StdfData::new(),
            reader: StdfReader::new(),
            part_counter: 0,
            current_wafer: String::new(),
        }
    }

    fn current_part_id(&self) -> String {
        format!(
            "{}_{}_{}",
            self.data.lot_id, self.current_wafer, self.part_counter
        )
    }

}

// ── Record parsers ──────────────────────────────────────────────────

fn parse_far(state: &mut ParserState, data: &[u8]) -> io::Result<()> {
    let mut r = Cursor::new(data);
    let cpu_type = state.reader.read_u1(&mut r)?;
    let _stdf_ver = state.reader.read_u1(&mut r)?;
    state.reader.little_endian = cpu_type != 1;
    Ok(())
}

fn parse_mir(state: &mut ParserState, data: &[u8]) -> io::Result<()> {
    let mut r = Cursor::new(data);
    let rd = &state.reader;
    let len = data.len() as u64;

    let _setup_t = rd.read_u4(&mut r)?;
    let start_t = rd.read_u4(&mut r)?;
    let _stat_num = rd.read_u1(&mut r)?;

    // Optional single-char fields
    let _mode_cod = if r.position() < len { rd.read_u1(&mut r)? } else { 0 };
    let _rtst_cod = if r.position() < len { rd.read_u1(&mut r)? } else { 0 };
    let _prot_cod = if r.position() < len { rd.read_u1(&mut r)? } else { 0 };
    let _burn_tim = if r.position() < len { rd.read_u2(&mut r)? } else { 0 };
    let _cmod_cod = if r.position() < len { rd.read_u1(&mut r)? } else { 0 };

    let lot_id = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };
    let part_typ = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };
    let _node_nam = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };
    let tstr_typ = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };
    let job_nam = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };
    let job_rev = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };

    // Additional optional fields
    let _sblot_id = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };
    let oper_nam = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };
    let _exec_typ = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };
    let _exec_ver = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };
    let test_cod = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };

    state.data.lot_id = lot_id;
    state.data.part_type = part_typ;
    state.data.job_name = job_nam;
    state.data.job_rev = job_rev;
    state.data.start_time = start_t as i64;
    state.data.tester_type = tstr_typ;
    state.data.operator = oper_nam;
    state.data.test_code = test_cod;

    Ok(())
}

fn parse_mrr(state: &mut ParserState, data: &[u8]) -> io::Result<()> {
    let mut r = Cursor::new(data);
    let finish_t = state.reader.read_u4(&mut r)?;
    state.data.finish_time = finish_t as i64;
    Ok(())
}

fn parse_wir(state: &mut ParserState, data: &[u8]) -> io::Result<()> {
    let mut r = Cursor::new(data);
    let rd = &state.reader;
    let len = data.len() as u64;

    let head_num = rd.read_u1(&mut r)?;
    let _site_grp = if r.position() < len { rd.read_u1(&mut r)? } else { 0 };
    let start_t = if r.position() < len { rd.read_u4(&mut r)? } else { 0 };
    let wafer_id = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };

    state.current_wafer = wafer_id.clone();
    state.data.wafers.push(WaferData {
        wafer_id,
        lot_id: state.data.lot_id.clone(),
        head_num: head_num as i64,
        start_time: start_t as i64,
        finish_time: 0,
        part_count: 0,
        good_count: 0,
        rtst_count: 0,
        abrt_count: 0,
    });

    Ok(())
}

fn parse_wrr(state: &mut ParserState, data: &[u8]) -> io::Result<()> {
    let mut r = Cursor::new(data);
    let rd = &state.reader;
    let len = data.len() as u64;

    let _head_num = rd.read_u1(&mut r)?;
    let _site_grp = if r.position() < len { rd.read_u1(&mut r)? } else { 0 };
    let finish_t = if r.position() < len { rd.read_u4(&mut r)? } else { 0 };
    let part_cnt = if r.position() < len { rd.read_u4(&mut r)? } else { 0 };
    let rtst_cnt = if r.position() < len { rd.read_u4(&mut r)? } else { 0 };
    let abrt_cnt = if r.position() < len { rd.read_u4(&mut r)? } else { 0 };
    let good_cnt = if r.position() < len { rd.read_u4(&mut r)? } else { 0 };

    if let Some(wafer) = state.data.wafers.last_mut() {
        wafer.finish_time = finish_t as i64;
        wafer.part_count = part_cnt as i64;
        wafer.good_count = good_cnt as i64;
        wafer.rtst_count = rtst_cnt as i64;
        wafer.abrt_count = abrt_cnt as i64;
    }

    Ok(())
}

fn parse_pir(state: &mut ParserState, data: &[u8]) -> io::Result<()> {
    let mut r = Cursor::new(data);
    let _head_num = state.reader.read_u1(&mut r)?;
    let _site_num = state.reader.read_u1(&mut r)?;
    state.part_counter += 1;
    Ok(())
}

fn parse_prr(state: &mut ParserState, data: &[u8]) -> io::Result<()> {
    let mut r = Cursor::new(data);
    let rd = &state.reader;
    let len = data.len() as u64;

    let head_num = rd.read_u1(&mut r)?;
    let site_num = rd.read_u1(&mut r)?;
    let part_flg = rd.read_u1(&mut r)?;
    let num_test = rd.read_u2(&mut r)?;
    let hard_bin = rd.read_u2(&mut r)?;
    let soft_bin = if r.position() < len { rd.read_u2(&mut r)? } else { 0 };
    let x_coord = if r.position() < len { rd.read_i2(&mut r)? } else { -32768 };
    let y_coord = if r.position() < len { rd.read_i2(&mut r)? } else { -32768 };
    let test_t = if r.position() < len { rd.read_u4(&mut r)? } else { 0 };

    let passed = (part_flg & 0x08) == 0;

    state.data.parts.push(PartData {
        part_id: state.current_part_id(),
        lot_id: state.data.lot_id.clone(),
        wafer_id: state.current_wafer.clone(),
        head_num: head_num as i64,
        site_num: site_num as i64,
        x_coord: x_coord as i64,
        y_coord: y_coord as i64,
        hard_bin: hard_bin as i64,
        soft_bin: soft_bin as i64,
        passed,
        test_count: num_test as i64,
        test_time: test_t as i64,
    });

    Ok(())
}

fn parse_ptr(state: &mut ParserState, data: &[u8]) -> io::Result<()> {
    let mut r = Cursor::new(data);
    let rd = &state.reader;
    let len = data.len() as u64;

    let test_num = rd.read_u4(&mut r)?;
    let _head_num = rd.read_u1(&mut r)?;
    let _site_num = rd.read_u1(&mut r)?;
    let test_flg = rd.read_u1(&mut r)?;
    let _parm_flg = rd.read_u1(&mut r)?;
    let result = if r.position() < len { rd.read_r4(&mut r)? as f64 } else { f64::NAN };
    let test_txt = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };
    let _alarm_id = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };

    // Optional fields
    let _opt_flag = if r.position() < len { rd.read_u1(&mut r)? } else { 0xFF };
    let _res_scal = if r.position() < len { rd.read_i1(&mut r)? } else { 0 };
    let _llm_scal = if r.position() < len { rd.read_i1(&mut r)? } else { 0 };
    let _hlm_scal = if r.position() < len { rd.read_i1(&mut r)? } else { 0 };
    let lo_limit = if r.position() < len { rd.read_r4(&mut r)? as f64 } else { f64::NAN };
    let hi_limit = if r.position() < len { rd.read_r4(&mut r)? as f64 } else { f64::NAN };
    let units = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };

    let passed = (test_flg & 0x80) == 0;

    state.data.tests.entry(test_num as i64).or_insert_with(|| TestDef {
        test_num: test_num as i64,
        test_name: test_txt.clone(),
        rec_type: "PTR".to_string(),
        lo_limit,
        hi_limit,
        units: units.clone(),
    });

    state.data.test_results.push(TestResult {
        lot_id: state.data.lot_id.clone(),
        part_id: state.current_part_id(),
        wafer_id: state.current_wafer.clone(),
        x_coord: state.data.parts.last().map_or(0, |p| p.x_coord),
        y_coord: state.data.parts.last().map_or(0, |p| p.y_coord),
        test_num: test_num as i64,
        test_name: state.data.tests.get(&(test_num as i64)).map_or_else(String::new, |t| t.test_name.clone()),
        rec_type: "PTR".to_string(),
        lo_limit,
        hi_limit,
        units,
        result,
        passed,
    });

    Ok(())
}

fn parse_mpr(state: &mut ParserState, data: &[u8]) -> io::Result<()> {
    let mut r = Cursor::new(data);
    let rd = &state.reader;
    let len = data.len() as u64;

    // Required fields
    let test_num = rd.read_u4(&mut r)?;
    let _head_num = rd.read_u1(&mut r)?;
    let _site_num = rd.read_u1(&mut r)?;
    let test_flg = rd.read_u1(&mut r)?;
    let _parm_flg = rd.read_u1(&mut r)?;
    let rtn_icnt = if r.position() < len { rd.read_u2(&mut r)? } else { 0 };
    let rslt_cnt = if r.position() < len { rd.read_u2(&mut r)? } else { 0 };

    // RTN_STAT: nibble array
    if rtn_icnt > 0 && r.position() < len {
        let num_bytes = (rtn_icnt as usize + 1) / 2;
        for _ in 0..num_bytes {
            if r.position() >= len { break; }
            let _ = rd.read_u1(&mut r)?;
        }
    }

    // RTN_RSLT: R*4 array
    let mut results = Vec::with_capacity(rslt_cnt as usize);
    for _ in 0..rslt_cnt {
        if r.position() >= len { break; }
        results.push(rd.read_r4(&mut r)? as f64);
    }

    // Optional fields (STDF V4 spec order)
    let test_txt = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };
    let _alarm_id = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };
    let _opt_flag = if r.position() < len { rd.read_u1(&mut r)? } else { 0xFF };
    let _res_scal = if r.position() < len { rd.read_i1(&mut r)? } else { 0 };
    let _llm_scal = if r.position() < len { rd.read_i1(&mut r)? } else { 0 };
    let _hlm_scal = if r.position() < len { rd.read_i1(&mut r)? } else { 0 };
    let lo_limit = if r.position() < len { rd.read_r4(&mut r)? as f64 } else { f64::NAN };
    let hi_limit = if r.position() < len { rd.read_r4(&mut r)? as f64 } else { f64::NAN };
    let _start_in = if r.position() < len { rd.read_r4(&mut r)? as f64 } else { 0.0 };
    let _incr_in = if r.position() < len { rd.read_r4(&mut r)? as f64 } else { 0.0 };

    // RTN_INDX
    for _ in 0..rtn_icnt {
        if r.position() >= len { break; }
        let _ = rd.read_u2(&mut r)?;
    }

    // UNITS
    let units = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };

    let passed = (test_flg & 0x80) == 0;

    state.data.tests.entry(test_num as i64).or_insert_with(|| TestDef {
        test_num: test_num as i64,
        test_name: test_txt.clone(),
        rec_type: "MPR".to_string(),
        lo_limit,
        hi_limit,
        units: units.clone(),
    });

    let result_val = results.first().copied().unwrap_or(f64::NAN);

    state.data.test_results.push(TestResult {
        lot_id: state.data.lot_id.clone(),
        part_id: state.current_part_id(),
        wafer_id: state.current_wafer.clone(),
        x_coord: state.data.parts.last().map_or(0, |p| p.x_coord),
        y_coord: state.data.parts.last().map_or(0, |p| p.y_coord),
        test_num: test_num as i64,
        test_name: state.data.tests.get(&(test_num as i64)).map_or_else(String::new, |t| t.test_name.clone()),
        rec_type: "MPR".to_string(),
        lo_limit,
        hi_limit,
        units,
        result: result_val,
        passed,
    });

    Ok(())
}

fn parse_ftr(state: &mut ParserState, data: &[u8]) -> io::Result<()> {
    let mut r = Cursor::new(data);
    let rd = &state.reader;

    let test_num = rd.read_u4(&mut r)?;
    let _head_num = rd.read_u1(&mut r)?;
    let _site_num = rd.read_u1(&mut r)?;
    let test_flg = rd.read_u1(&mut r)?;

    let passed = (test_flg & 0x80) == 0;

    state.data.tests.entry(test_num as i64).or_insert_with(|| TestDef {
        test_num: test_num as i64,
        test_name: String::new(),
        rec_type: "FTR".to_string(),
        lo_limit: f64::NAN,
        hi_limit: f64::NAN,
        units: String::new(),
    });

    state.data.test_results.push(TestResult {
        lot_id: state.data.lot_id.clone(),
        part_id: state.current_part_id(),
        wafer_id: state.current_wafer.clone(),
        x_coord: state.data.parts.last().map_or(0, |p| p.x_coord),
        y_coord: state.data.parts.last().map_or(0, |p| p.y_coord),
        test_num: test_num as i64,
        test_name: String::new(),
        rec_type: "FTR".to_string(),
        lo_limit: f64::NAN,
        hi_limit: f64::NAN,
        units: String::new(),
        result: f64::NAN,
        passed,
    });

    Ok(())
}

fn parse_hbr(state: &mut ParserState, data: &[u8]) -> io::Result<()> {
    let mut r = Cursor::new(data);
    let rd = &state.reader;
    let len = data.len() as u64;

    let _head_num = rd.read_u1(&mut r)?;
    let _site_num = rd.read_u1(&mut r)?;
    let bin_num = rd.read_u2(&mut r)?;
    let bin_cnt = rd.read_u4(&mut r)?;
    let bin_pf = if r.position() < len { (rd.read_u1(&mut r)? as char).to_string() } else { String::new() };
    let bin_nam = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };

    state.data.bins_hard.insert(
        bin_num as i64,
        BinData {
            bin_num: bin_num as i64,
            bin_count: bin_cnt as i64,
            bin_name: bin_nam,
            bin_pf,
        },
    );

    Ok(())
}

fn parse_sbr(state: &mut ParserState, data: &[u8]) -> io::Result<()> {
    let mut r = Cursor::new(data);
    let rd = &state.reader;
    let len = data.len() as u64;

    let _head_num = rd.read_u1(&mut r)?;
    let _site_num = rd.read_u1(&mut r)?;
    let bin_num = rd.read_u2(&mut r)?;
    let bin_cnt = rd.read_u4(&mut r)?;
    let bin_pf = if r.position() < len { (rd.read_u1(&mut r)? as char).to_string() } else { String::new() };
    let bin_nam = if r.position() < len { rd.read_cn(&mut r)? } else { String::new() };

    state.data.bins_soft.insert(
        bin_num as i64,
        BinData {
            bin_num: bin_num as i64,
            bin_count: bin_cnt as i64,
            bin_name: bin_nam,
            bin_pf,
        },
    );

    Ok(())
}

// ── Main parse function ─────────────────────────────────────────────

/// Parse an STDF file (supports .stdf and .stdf.gz).
pub fn parse_stdf<P: AsRef<Path>>(path: P) -> io::Result<StdfData> {
    let path = path.as_ref();
    let file = File::open(path)?;

    // Detect gzip by extension
    let is_gz = path
        .to_str()
        .map_or(false, |s| s.ends_with(".gz"));

    if is_gz {
        let decoder = GzDecoder::new(file);
        let mut buf_reader = BufReader::new(decoder);
        parse_stream(&mut buf_reader)
    } else {
        let mut buf_reader = BufReader::new(file);
        parse_stream(&mut buf_reader)
    }
}

fn parse_stream<R: Read>(reader: &mut R) -> io::Result<StdfData> {
    let mut state = ParserState::new();

    // Read the initial header to detect endianness before creating the reader
    // First 4 bytes: rec_len(2) + rec_typ(1) + rec_sub(1)
    let mut header_buf = [0u8; 4];
    if reader.read_exact(&mut header_buf).is_err() {
        return Ok(state.data);
    }

    // Try little-endian first
    let rec_len = u16::from_le_bytes([header_buf[0], header_buf[1]]);
    let rec_typ = header_buf[2];
    let rec_sub = header_buf[3];

    // FAR should be the first record
    if (rec_typ, rec_sub) == (REC_FAR.0, REC_FAR.1) {
        let mut rec_data = vec![0u8; rec_len as usize];
        reader.read_exact(&mut rec_data)?;
        parse_far(&mut state, &rec_data)?;
    }

    // Main parse loop
    loop {
        let mut header_buf = [0u8; 4];
        if reader.read_exact(&mut header_buf).is_err() {
            break; // EOF
        }

        let rec_len = if state.reader.little_endian {
            u16::from_le_bytes([header_buf[0], header_buf[1]])
        } else {
            u16::from_be_bytes([header_buf[0], header_buf[1]])
        };
        let rec_typ = header_buf[2];
        let rec_sub = header_buf[3];

        // Read record data
        let mut rec_data = vec![0u8; rec_len as usize];
        if reader.read_exact(&mut rec_data).is_err() {
            break; // Truncated record at EOF
        }

        let result = match (rec_typ, rec_sub) {
            (0, 10) => parse_far(&mut state, &rec_data),
            (1, 10) => parse_mir(&mut state, &rec_data),
            (1, 20) => parse_mrr(&mut state, &rec_data),
            (1, 40) => parse_hbr(&mut state, &rec_data),
            (1, 50) => parse_sbr(&mut state, &rec_data),
            (2, 10) => parse_wir(&mut state, &rec_data),
            (2, 20) => parse_wrr(&mut state, &rec_data),
            (5, 10) => parse_pir(&mut state, &rec_data),
            (5, 20) => parse_prr(&mut state, &rec_data),
            (15, 10) => parse_ptr(&mut state, &rec_data),
            (15, 15) => parse_mpr(&mut state, &rec_data),
            (15, 20) => parse_ftr(&mut state, &rec_data),
            _ => Ok(()), // Skip unknown records
        };

        if let Err(_) = result {
            // Skip problematic records, continue parsing
            continue;
        }
    }

    Ok(state.data)
}
