# マルチユーザー解析 設計 (2026-07-04)

## 背景と要件

STDF解析は共有マシン1台(単一ノード)で完結していた。数人でアドホック解析を
できるようにする。設計対話で確定した前提:

- 生データ(STDF/Parquet)は社内LAN内なら共有可。社外持ち出しのみ禁止
- データ規模は数十GB(test_dataが支配的)
- **Phase 1 の利用者はVSCodeで解析する約3名**。ブラウザだけのメンバーやGUIは将来
- 既存のFastAPI+Reactダッシュボードへの統合は将来要件(今は作らない)
- 選定の決め手: **各PCの環境準備を最小化したい**

## 検討した3案

| 案 | 概要 | 採否 |
|---|---|---|
| A: 共有フォルダ+ローカルDuckDB | data/ を読み取り専用SMB共有、各PCの config が UNC を指す | パワーユーザー向け代替として文書化のみ |
| B: 常駐クエリサービス | 共有マシンにHTTP APIを常駐、結果だけ返す | **採用** |
| C: 差分同期(stdf pull) | 各PCへParquetをミラー | 見送り(初回数十GB・鮮度管理) |

案Bの採用理由: 各PCは requests+pandas + クライアント1ファイルで済む(repo・
DuckDB・共有権限すら不要)。歩留まり定義(views.py)の一本化が全員に保証される。
将来のダッシュボード統合の土台になる。

## 決定事項

1. **ルーター設計**: 本体は `APIRouter`(stdf_platform/server/app.py)。
   `stdf serve` は単体起動の入れ物。将来ダッシュボードが `include_router` で吸収
2. **1リクエスト=1 AnalysisSession**: 既存部品をそのまま利用。接続共有・
   ロック管理なし、リクエスト間完全分離
3. **セキュリティ(実験で動作確認済み・DuckDB 1.4)**:
   - ビュー登録後に `allowed_directories=[data_dir]` → `enable_external_access=false`
     → `lock_configuration=true`(ビューのParquetスキャンは通り、他のファイルI/Oと
     設定変更はブロック)
   - `duckdb.extract_statements` で単一SELECT文のみ許可(多層防御)
   - 結果行数上限 `server.max_rows`(既定10,000、truncatedフラグ)
   - 認証はなし(社内LAN・数人)。必要になればプロキシ/トークン
4. **クライアント**: `client/stdf_client.py` は stdf_platform 非依存の単体1ファイル。
   `q()` / `to_csv()` / `views()` のみ
5. **ブラウザ向けSQLコンソール画面は作らない**(Phase 1 利用者は全員VSCode。
   Web UIを過去に削除した経緯とも整合)

## スコープ外(将来)

- GUIダッシュボード統合(routerを既存FastAPIにマウント、Reactから /api/* を呼ぶ)
- 認証・監査ログ
- 案C(ローカル差分同期)
