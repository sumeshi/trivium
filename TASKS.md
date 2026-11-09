# Trivium — タスク分解（v2）

> 本書は「このチェックアウトで再現可能に進める」ための分解である。
> 履歴上の実装報告（DuckDB/Tauri 2/HTTP API/Python SDK/E2E 96件等）は現ソースに存在しないため、
> 完了条件として扱わない。

## 0. 現在地（2026-11-09 確認済み）

- ブランチ `main` のみ、`00a9c1a`、バージョン `0.1.4`。`develop` なし。CI は `build.yml`（Windows exe のみ）。
- Tauri 1 + Polars 0.40 + Svelte 4 + sled。`docs/` なし。`scripts/` は `gen_fixture.py`(I1) / `check_rust_size.sh`(I3) を追加済み。
- **テストが一切無い**: `#[cfg(test)]` 0 件、`src-tauri/tests/` 無し、package.json に Playwright/vitest なし。
- サーバー実装（HTTP API）は存在しない。バイナリは Tauri デスクトップのみ。
- **ローカル環境の制約**: webkit2gtk-4.0 / gtk+-3.0 の開発パッケージが無く sudo 不可。
  ため `src-tauri`（Tauri アプリ）のビルドはローカルで検証できない（CI のみ）。
  このため純粋ロジックはワークスペースの `crates/engine`（クレート名 `trivium-engine`）へ分離し、
  `cargo test -p trivium-engine` で検証する（R0a）。Cargo.lock はコミットされていない。

## 1. v1 からの妥当性整理（本書を書き直した理由）

1. **存在しない基盤を完了条件に参照していた**。v1 は各タスクで「cargo test・E2E の成否を記録」と
   要求したが、テストも E2E も fixtures も無い。→ I 層（足場）を先頭に置き、全タスクの完了条件は
   実在するコマンドのみにした。
2. **判断タスクと実装タスクを混ぜていた**。ADR（エンジン選択等）は弱い実行者に渡せない。
   → D 層（判断専用）へ分離し、D 層未決でも F 層が進むよう、デフォルト判断を先に固定した。
3. **毎タスク手動 GUI 確認を要求していた**。自動スモーク（I5）に置換した。
4. **「サーバー .deb」を範囲に含めていた**。サーバー実装が現ツリーに無いため X 層（保留）へ移した。
5. **非目標（Sigma、100GB、複数端末）をタスク列に混ぜていた**。§11 へ分離した。
6. **現ソースの実バグ 3 件（B1〜B3）を発見**。これらは新機能より先に直す。

### 発見済みバグ（要修正）

- **B1 列絞り込みが検索に効いていない** → 修正済み（`d225992`）。
  `src/lib/backend.ts:127` が `visible_columns` を送るのに対し Rust 側は `columns` を期待し、
  serde が無視するため常に全列検索になっていた。
- **B2 IOC タグ付与の表示/出力不一致** → 修正済み（`c8b0ef2`、R5 の単一化の上で適用）。
  統一ルール: 空でないユーザーフラグがフラグ列優先（メモのみのエントリは IOC を阻害しない）、
  タグは IOC 一致なら常に付与。表示・出力・件数が一致することを `ioc_parity.rs` が固定。
- **B3 1 行のフラグ更新が全走査を起こす** → 修正済み（`aebde10`）。
  `update_flag` が毎回 `calculate_ioc_applied_records`（全行×IOC×列）を呼んでいた。
- **B4 検索キャッシュが列構成を見ていない** → 修正済み（`cf48a61`、I5 のスモークが発見）。
  キャッシュが列セットを記録しておらず、列制限付き検索が別列構成のキャッシュを
  再利用できた。`set_hidden_columns` がキャッシュをクリアするため GUI では隠れていた。

## 2. タスクカードの書式（以後、追加タスクもこれに従う）

```
### <ID>: <タイトル>
- 前提: <完了済みID>（未完了なら着手不可）
- 対象: <正確なファイルパス>（現状行数）
- 手順: 番号付きの逐次手順。判断を要する分岐を含ませない。
- 完了条件: <実行コマンド> → <期待出力>。機械的に判定できること。
- 禁止: <触ってはいけないファイル・やってはいけないこと>
```

**全タスク共通ルール**（各カードには書かない）:

- 1 タスク = 1 コミット。コミットメッセージは `fix:`/`feat:`/`refactor:`/`test:`/`chore:` + ID。
- 追加・変更する Rust は 1 ファイル 400 行以内（下記 §2.1）。

### 2.1 Rust 規模規律

- **単一ファイル 500 行を上限**（`scripts/check_rust_size.sh` が機械的に強制）。
  新規ファイルは 400 行以内を目指す。行数基準（トークン数でなく）で判定する。
- `#[cfg(test)]` の単体テストは行数起算から除外しない。大きくなったら
  `crates/engine/tests/` へ統合テストとして分ける。
- 例外は自動生成コードのみ。現状 `commands/rows.rs`(468 行) は上限未満だが、
  R0b で engine へ移動する際に必ず分割する。
- ワークスペース構成（R0a 後）:

```
/Cargo.toml               # workspace（src-tauri + crates/engine）
crates/engine/src/        # trivium-engine: Tauri 非依存の処理エンジン
  lib.rs
  store.rs                # ProjectsStore
  error.rs flags.rs value_utils.rs models.rs
  ioc/                    # R5 後: mod.rs + evaluate.rs
  search/                 # R1 後: mod.rs tokenize.rs rpn.rs mask.rs text.rs
  import/                 # R3 後: csv.rs（F22-F25 で拡張）
  export/                 # R4 後: csv.rs（F16/F27 で拡張）
  ops/                    # R0b 後: query/create/load/export 相当の engine 関数
  analysis/               # F7 以降: stats.rs dupes.rs time.rs
  condition/              # F1 以降: model.rs eval.rs
src-tauri/src/
  main.rs                 # 起動のみ
  state.rs                # AppState（tauri setup）のみ
  commands/               # 5〜10 行の薄いラッパのみ（R0b 後 400 行未満）
```
- コミット前に必ず: `scripts/check_rust_size.sh` → `cargo test -p trivium-engine`（R0a 以降。
  それ以前は `cargo test`）→ `npm run lint`。いずれか失敗したらコミットしない。
  `src-tauri` 側のコンパイルはローカルで検証できないため CI で確認する（手元では
  `use` パスの grep 検証で代用する）。
- 「〜だと思われる」「おそらく」をコミットメッセージと完了報告に書かない。
  確認できていないことは「未確認」と書く。
- 完了報告には実行したコマンドとその結果だけを書く。履歴上の過去報告を根拠にしない。

## 3. D 層 — 判断タスク（実行者に渡さない。人間または強いエージェントが決める）

### D1: 検索欄と条件モデルの関係

- 選択肢: (a) 現行文字列検索を維持し条件エディタと並存、(b) 条件モデルに一本化して検索欄は
  条件の簡易入力に変換、(c) 検索欄廃止
- 推奨: (a)。既存 IOC クエリ（`iocs.json`）の互換を壊さない。
- 影響: F4 以降の UI 設計。未決でも F1〜F3（モデル・評価器）は着手可能。

### D2: Tauri 1 継続か 2 へ移行するか（Node 18→22 を含む）

- 推奨: しばらく Tauri 1 継続。deb は v1 の `tauri build --bundles deb` で生成可能。
  F 層・R 層のどのタスクも Tauri 2 を前提にしない。
- 再判断の契機: HTTP API / Python SDK（X2）に着手するとき。

### D3: エンジン共有 API の公開面（Polars 継続 vs DuckDB 移行を含む）

- 推奨: 当面 Polars 継続。条件モデル（F1/F2）は行単位の述語評価として書き、
  エンジン実装に依存させない。DuckDB 再評価は X2 と同時に。
- R0 で engine を Tauri から分離するため、将来の差し替えコストは下がる。

> D 層は未決でも R 層・B 層・F1〜F3 を block しない。block するのは明記したタスクだけ。

## 4. I 層 — 足場（最初にやる。これが無いと完了条件が全部嘘になる）

### I1: 合成データ生成スクリプト

- 前提: なし
- 対象: 新規 `scripts/gen_fixture.py`
- 手順:
  1. `argparse` で `--rows N --seed 42 --out PATH --variant {ascii,utf16,wide}` を取る
  2. 列: `ts`(ISO8601。1 行おきにミリ秒/ナノ秒)、`EventID`(4624/4625/4688/4103 を分布付き)、
     `host`(WS01..WS50)、`user`、`proc`、`path`、`msg`（カンマ・引用符含みを混ぜる）
  3. 既知の重複（同一 `(user,proc)` の 2 連続行を 1%）と時刻ギャップ（30 分以上の空白を数カ所）を注入
  4. `utf16` は UTF-16LE BOM 付き、`wide` は 20 列に増やす
  5. 生成物は `fixtures/` に置く（`.gitignore` に入れず、小さいものはコミット）
- 完了条件: `python3 scripts/gen_fixture.py --rows 1000 --seed 42 --out fixtures/basic.csv` が
  成功し、`wc -l fixtures/basic.csv` が 1001（ヘッダ+1000行）。seed 固定で再生成すると
  バイト列が完全一致する（`sha256sum` で確認）。
- 禁止: アプリ本体のコードに触れない。

### I2: cargo test の雛形（純粋関数のテストのみ）

- 前提: R0a（テストは `trivium-engine` クレートに置く。src-tauri はローカルでビルドできないため）
- 対象: 新規 `crates/engine/tests/search_test.rs`、`crates/engine/tests/flags_test.rs`
- 手順:
  1. `tokenize_search_query` / `to_rpn` / `build_search_mask_boolean` について
     README「Search Syntax」節の全例（`malware beacon`、`error|warn`、`beacon -test`、
     `eventid:4624 user:administrator`、`command:"powershell -enc"`、`"-keyword"`、
     列引き継ぎ `com:WS01|WS02`）を表駆動テストにする
  2. `normalize_flag_value` / `severity_rank` の全分岐
  3. `anyvalue_to_search_string` の各 AnyValue バリアント
- 完了条件: `cargo test -p trivium-engine` が 0 失敗。テストは現在の実装の挙動を固定する（良し悪しの判断はしない）。
- 禁止: `crates/engine/src/`（実装）の変更。期待値は実装に合わせ、仕様判断は D1 に残す。

### I3: Rust 規模ガード

- 前提: なし
- 対象: 新規 `scripts/check_rust_size.sh`
- 手順: `src-tauri/src` 配下の `*.rs` を `wc -l` し、500 行超で exit 1 する。
  現状の `commands/rows.rs`(468) は 500 未満のため通る。
- 完了条件: `scripts/check_rust_size.sh` が exit 0。`touch` で 501 行のダミーを
  `src-tauri/src/` に置いた場合のみ exit 1（検証後削除）。
- 禁止: `*.rs` の実装変更。

### I4: エンジンベンチバイナリ（R0b の後）

- 前提: R0b
- 対象: 新規 `crates/engine/src/bin/bench.rs`（または `cargo test --release` のみで足りるなら
  `crates/engine/tests/bench.rs` に `#[ignore]` テストとして置く）
- 手順:
  1. fixtures 100k / 400k / 2M に対し: 取込(create)、初回ソート、抽出書出、
     検索切替（2 条件の交互）、IOC 100 件 + 1 件追加 を実行し所要時間を出力
  2. 初回（キャッシュ無し）と 2 回目（キャッシュ有り）を分けて出力
  3. 結果を `docs/performance.md` の表に追記
- 完了条件: `bench` 実行で表が埋まる。数値の良し悪しは問わない（P 層の before になる）。
- 禁止: 100GB 級の計測（§11 非目標）。

### I5: 往復スモークの自動化 → 完了（`a2fe91d`）

- 対象: `crates/engine/tests/smoke.rs`
- 実装メモ: fixtures は `*.csv` が gitignore 済みで CI の新規チェックアウトに存在しないため、
  `scripts/gen_fixture.py` ではなく**テスト内で決定的な CSV を inline 生成**した。
  カバー: 案件作成/読込、検索（列スコープ・列制限・AND/NOT/OR・列引き継ぎ）、
  ソート+ページング、フラグ更新+フィルタ、IOC 適用（ユーザーフラグ優先・タグ・件数）、
  出力→再取込の往復、案件削除。I5 実行中に B4（キャッシュの列キー不在）を発見し修正。
- 完了条件: `cargo test -p trivium-engine --test smoke` が 0 失敗（7 テスト、達成済み）。
  以後、全 F/R タスクの完了条件にこのスモークの通過を含める（手動 GUI 確認の代わり）。

## 5. B 層 — バグ修正（I2 の後、R 層より先）

### B1: 検索列の不整合

- 前提: なし
- 対象: `src/lib/backend.ts:127` と `src-tauri/src/commands/rows.rs:45-63`
- 手順: いずれか一方に合わせる（`visible_columns` を Rust 側で `#[serde(rename)]` するか、
  TS 側を `columns` に直す）。列の非表示を切り替えたとき検索結果が変わることを I5 に追加。
- 完了条件: `cargo test`（rename した側）+ `npm run lint` + I5 追加ケース通過。
- 禁止: `search.rs` 本体の変更。

### B2: IOC タグの表示/出力不一致

- 前提: R5（評価の単一化後でないと二重修正になる）
- 対象: `crates/engine/src/ioc/evaluate.rs` 統合後の query 系・export 系・flags 系
- 手順: 「ユーザーフラグ優先、タグは IOC 一致なら常に付与」に一本化し、
  表示・出力・`calculate_ioc_applied_records` が同一結果を返すことを I5 で検証。
- 完了条件: I5 に「ユーザーフラグ行 + IOC 一致」ケースを追加して通過。
- 禁止: セマンティクスの新設計（D 層でない）。既存動作のどちらかに合わせるだけ。

### B3: update_flag の全走査

- 前提: なし
- 対象: `src-tauri/src/commands/flags.rs`、`crates/engine/src/ioc.rs`（R0a 前
  に着手する場合は `src-tauri/src/ioc.rs`）
- 手順:
  1. `calculate_ioc_applied_records` の呼び出しを 1 行更新の経路から外す
  2. IOC 適用件数は IOC 保存時（`save_iocs`）に一度計算して `ProjectMeta` に持たせる
- 完了条件: 40 万行案件でフラグ 1 件更新が 100ms 以内（I4 に計測ケース追加）。
- 禁止: `ioc_flag_cache` の形式変更（R5 でやる）。

## 6. R 層 — リファクタ（規模規律の適用。F 層の前に済ませる）

### R0a: ワークスペース分割（trivium-engine クレートの作成）

- 前提: なし（I2 の前）
- 対象: 新規 `/Cargo.toml`、`crates/engine/`、`src-tauri/Cargo.toml`、`src-tauri/src/main.rs`、
  `src-tauri/src/state.rs`、`src-tauri/src/commands/*.rs`
- 手順:
  1. ルート `Cargo.toml` に `[workspace]`（members = `src-tauri`, `crates/engine`、resolver = "2"）
  2. 移動するモジュール（tauri 非依存）: `error.rs`, `flags.rs`, `ioc.rs`, `models.rs`,
     `project_io.rs`, `search.rs`, `storage.rs`, `value_utils.rs` → `crates/engine/src/` へ
     `git mv`。`lib.rs` に `pub mod` 宣言
  3. `state.rs` の `ProjectsStore`（tauri 非依存）→ `crates/engine/src/store.rs` へ移動。
     `state.rs` には `AppState` のみ残す
  4. `crates/engine/Cargo.toml` に依存を移す: anyhow, chrono(serde), csv, parking_lot,
     polars(lazy,parquet,serde,fmt), serde(derive), serde_json, sled, thiserror, uuid(v4,serde)
  5. `src-tauri/Cargo.toml` から engine 側にのみ必要な依存（csv, sled）を削除し、
     `trivium-engine = { path = "../crates/engine" }` を追加
  6. `main.rs` の `mod` 宣言を移動分だけ削除。commands/*.rs と state.rs の
     `use crate::{...}`（移動モジュール参照）を `use trivium_engine::{...}` に置換
- 完了条件: `cargo check -p trivium-engine` と `cargo test -p trivium-engine` が通過。
  `grep -rn "use crate::" src-tauri/src/` の結果が `crate::commands` / `crate::state` のみを
  参照している。`src-tauri` のビルドは CI で確認（ローカル不可）。
- 禁止: 挙動の変更。モジュールの中身の編集（移動と `use` 置換のみ）。

### R0b: コマンド本体の engine 移動

- 前提: R0a
- 対象: `src-tauri/src/commands/*.rs` の関数本体 → `crates/engine/src/`
- 手順:
  1. 各 `#[tauri::command]` の中身を、`&ProjectsStore` または `PathBuf` を受け取る
     `engine::` 関数（`crates/engine/src/ops/` に配置）へ移動。戻り値は `Result<T, AppError>`
  2. コマンドは「State を展開して engine 関数を呼び、`String` に直す」だけの 5〜10 行にする
  3. この時点ではファイル分割（R2〜R4）はしない。移動のみ
- 完了条件: `cargo test -p trivium-engine` 通過。`commands/` 配下の合計行数が 400 行未満。
  I5（スモーク）が engine 関数を直接呼べる形になること。
- 禁止: 挙動の変更（引数・戻り値の型は現状維持）。

### R1: search.rs の分割 → 完了（`48cd8d3`）

- 前提: R0a（I2 と並行可）
- 対象: `crates/engine/src/search.rs`(392 行) → `crates/engine/src/search/{mod.rs, tokenize.rs, rpn.rs, mask.rs, text.rs}`
- 手順: `SearchToken` と字句解析は `tokenize.rs`、`to_rpn` は `rpn.rs`、
  `build_search_mask_boolean` は `mask.rs`、`build/ensure_searchable_text` は `text.rs`。
  `mod.rs` は re-export のみ（50 行以内）。`use` パスの更新のみでロジック変更はしない。
- 完了条件: `cargo test` 通過（I2 の表駆動テストが通れば移動検証になる）。各ファイル 200 行未満。
- 禁止: トークナイザの挙動変更。

### R2: rows 系の分割 → 完了（`815ed57`）

- 前提: R0b
- 手順: R0b 後に query 系 engine ファイルが 200 行超なら `crates/engine/src/ops/query/` へ
  ソート・IOC 適用・ページングに分割。200 行未満なら何もしない。
- 完了条件: `scripts/check_rust_size.sh` 通過。I5 通過。

### R3: 取込の分離 → 完了（`ee92a42`）

- 前提: R0b
- 対象: `crates/engine/src/ops/projects.rs`（create_project 本体）→ 新規 `crates/engine/src/import/csv.rs`
- 手順: `create_project` の CSV 読込〜trivium-* 列取込〜`__rowid` 付与までを
  `import::csv::load(path) -> (DataFrame, HashMap<usize, FlagEntry>)` として切り出す。
- 完了条件: I5 通過。該当呼び出し側が 200 行未満。

### R4: 出力の分離 → 完了（`42a1513`）

- 前提: R0b, R5
- 対象: `crates/engine/src/ops/export.rs` → 新規 `crates/engine/src/export/csv.rs`
- 完了条件: I5 通過（出力→再取込の復元はスモークが検証済み）。

### R5: IOC 評価の単一化（最重要リファクタ）

- 前提: R0b, I2
- 対象: 現在 3 箇所にある重複実装 — `crates/engine/src/ioc.rs`（row_contains_query と
  calculate_ioc_applied_records）、query 系（rows 相当）の IOC ブロック、
  export 系の IOC ブロック → 新規 `crates/engine/src/ioc/evaluate.rs`
- 手順:
  1. 「IOC エントリ一覧 × DataFrame → 行ごとの (flag, tags)」を 1 関数にまとめる
  2. query / export / flags 経路はそれを呼ぶだけにする
  3. I2 に「3 呼び出し経路で同一入力に同一結果」のパリティテストを追加
- 完了条件: パリティテスト通過。`grep -rn "tokenize_search_query" crates/engine/src | wc -l` が
  `ioc/evaluate.rs` と query 系の 2 箇所以下。
- 禁止: 評価順序（severity 降順）の変更。

## 7. F 層 — 機能（各カードは 1 PR 相当。D1 の推奨 (a) を前提）

### 条件モデル（IRFlow 差分の中核。この順で）

#### F1: 条件モデルの型定義と直列化 → 完了（`70a737c`）

- 前提: R0
- 対象: `crates/engine/src/condition/model.rs`（Tauri 非依存。カード記載の `src-tauri/src/condition` は R0 後の配置に合わせた）
- 手順:
  1. `Condition { column, op, values, negated }`（op: Eq/Ne/Gt/Lt/Ge/Le/Contains/StartsWith/EndsWith/Regex/In）、
     `Group { kind: And|Or, children: Vec<Node> }`、`Node = Condition|Group` を serde で JSON 往復可能に定義
  2. serde の逆直列化ラウンドトリップテストのみ。評価器はまだ作らない
- 完了条件: `cargo test` のラウンドトリップテスト通過（30 ケース以上）。
- 禁止: 検索バー構文との変換（F3 でやる）。

#### F2: 条件評価器 → 完了（`b3a736a`）

- 前提: F1, R5
- 対象: `crates/engine/src/condition/eval.rs`
- 手順:
  1. 行の値（`&HashMap<String, Value>` または Series 参照）に対する `eval(Node, row) -> bool`
  2. 数値比較は `f64` に行く前に `i64` 比較を試す（精度。F28 と同じ規則）
  3. 既存 `build_search_mask_boolean` の Contains/列スコープ/AND/OR/NOT と
     同一意味のケースでパリティテスト（部分一致の意味が同じであることの証明）
- 完了条件: パリティテスト通過。`cargo test` 0 失敗。

#### F3: 条件 → マスクへの統合（query_rows の一本化） → 完了（`c893af7`）

- 前提: F2, B1
- 対象: `engine/query`（R0/R2 で分割後のファイル）
- 手順: `query_project_rows` が `search: Option<String>`（旧構文）と
  `condition: Option<Node>`（新モデル）のどちらも受け取れるようにし、
  内部で両者を `Node` に変換してから F2 で評価する。
- 完了条件: 旧構文の I2 テストが全部通ったまま（互換維持）、I5 通過。
- 禁止: 旧構文の破壊（D1 が (b)/(c) を選ぶまで）。

#### F4: 条件エディタ UI（フラット） → 実装済み（`fd092c5`）、GUI 操作は未確認

- 前提: F3
- 対象: 新規 `src/lib/components/project_view/ConditionEditor.svelte`、
  `src/lib/stores/condition.ts`、`src/lib/backend.ts`（payload に `condition` を追加）
- 手順: 条件行の追加・削除、列は `projectDetail.columns` のドロップダウン、
  演算子ドロップダウン、値入力、行間の AND/OR 切替（フラット）。プレビュー件数は
  `queryProjectRows` の `total_filtered_rows` を流用。検索バーはそのまま残す（D1(a)）。
- 完了条件: `npm run lint` 通過 + I5 通過（engine 経路）。手動確認は 1 回だけ
  （表示・絞り込み・件数表示を確認した旨を報告）。
- 禁止: スタイリングの作り込み（skin は T7 で分離するため class 命名のみ）。

#### F5: ネストグループ対応（エディタ） → 実装済み（`434f9cb`）、GUI 操作は未確認

- 前提: F4, F1
- 手順: グループの畳み込み/展開、グループ内 AND/OR、ドラッグによる移動は不可（ボタン移動のみ）。
  上位・下位の移動ボタンで足りる。
- 完了条件: F4 と同じ。I5 に入れ子条件のケース追加。

#### F6: 型付き値入力（数値・日時） → 実装済み（`711eca3`）、GUI 操作は未確認

- 前提: F4
- 手順: 列の型推論（F7 と共通の `analysis` 推論を使う。F7 が先なら参照、未なら暫定で
  全件走査 1 回）で数値列は数値入力、日時列は `datetime-local` 入力に切替。
- 完了条件: F4 と同じ。`1700000000123456789`（ナノ秒）が丸まらないことを I5 で検証。

### 分析

#### F7: 列統計エンジン + コマンド → 完了（`49b748c`）

- 前提: R0, I1
- 対象: `crates/engine/src/analysis/stats.rs`（カード記載の `src-tauri/src/analysis/stats.rs` は R0 後の配置）。コマンドは `src-tauri/src/commands/stats.rs` の `column_stats`
- 手順: 列ごとに「型推論(null/int/float/datetime/string)、欠損数、ユニーク数、上位 20 値、
  希少値(頻度 1〜N) 件数と値」を 1 走査で計算。DataFrame 全件舐めでよい（100GB 非目標）。
- 完了条件: fixtures で欠損数・ユニーク数が `python3 -c` で別計算した値と一致（I5 に組み込み）。

#### F8: 列統計パネル UI → 実装済み（`80e7d5d`）、GUI 操作は未確認

- 前提: F7
- 対象: 新規 `src/lib/components/project_view/StatsPanel.svelte`
- 手順: 列選択 → 統計表示 → 値クリックでその値に絞り込み（`condition` に In 条件を発行）。
- 完了条件: F4 と同じ。希少値から絞り込みまでの操作が成立。

#### F9: 重複 / 単独値抽出 → 完了（`f31e9be`）

- 前提: F7（推論を共用）
- 対象: `crates/engine/src/analysis/dupes.rs`。コマンドは `src-tauri/src/commands/dupes.rs` の `duplicate_rows`
- 手順: 指定列（複数可）をキーに重複行のみ / 単独値のみの行集合を返すコマンド。
  重複件数を保持（3 回出現は 3 行として返す）。
- 完了条件: fixtures の意図的重複（1%）が検出され件数一致（I5 に組み込み）。

#### F10: ヒストグラム → 実装済み（`250dca0`）、GUI 操作は未確認

- 前提: F7
- 手順: 数値列・日時列のバケット集計（バケット数は UI 指定、既定 50）。
- 完了条件: バケット件数の合計 = 検索条件一致件数（I5）。

#### F11: 時間解析（ギャップ・セッション・バースト） → 実装済み（`893bb8a`）、GUI 操作は未確認

- 前提: F7（日時推論）
- 手順: ギャップ（指定秒以上の間隔）、セッション（指定分で切断）、バースト（指定秒窓の件数超）。
  それぞれ「期間/件数」のリストを返し、UI から期間クリックで絞り込み。
- 完了条件: fixtures の意図的ギャップが検出される（I5）。

#### F12: 行詳細 + 周辺イベント → 実装済み（`1e58336`）、GUI 操作は未確認

- 前提: F3
- 手順: 行クリックで隠し列を含む全列を表示（既存 `ExpandedCellDialog` を拡張 or 新規 Dialog）、
  「前後 N 行 / 時刻窓 ±秒」の周辺行取得コマンド。
- 完了条件: 隠し列の値が詳細に現れ、周辺行が時刻順に並ぶ（I5）。

#### F13: 戻る/進む + 案件状態復元 → 実装済み（`5cd3310`）、GUI 操作は未確認

- 前提: F4
- 手順: 条件適用を履歴スタックに積み、戻る/進むで条件と offset を復元。
  条件・列設定・ソート・スクロール位置を案件ごとに `localStorage` に保存し再読込で復元。
- 完了条件: 手動確認 1 回（スモークは UI のため対象外。操作手順を完了報告に記録）。
- 禁止: サーバー側保存（X 層の複数端末は低優先度）。

#### F14: 案件間検索 → 完了（`7991818`）

- 前提: F3, R0
- 手順: 複数案件 ID を受け取り同一条件で各案件のヒット件数とページング行を返すコマンド。
- 完了条件: 2 案件で同一条件のヒット件数が個別検索と一致（I5）。

#### F15: 差分比較 → 完了（`05f5db0`）

- 前提: F14
- 手順: 指定列をキーにした一致/片側のみ/重複件数つき差分。比較結果の出力（CSV）を含む。
- 完了条件: 重複件数が保存されること（3 回 vs 1 回が「1 回差」でなく「2 行残」として出る）。

#### F16: HTML レポート出力 → 完了（`ce07880`）

- 前提: F7, F15
- 手順: 条件・ヒット件数・統計・注釈済み行（表示列・並び順反映）を単一 HTML に出力。
- 完了条件: 出力 HTML に script 依存なし。件数が query と一致。

### 注釈・グルーピング

#### F17: 階層グルーピング → 完了（`c060e44`）

- 前提: F7
- 手順: 複数列の順でグループ木（件数つき）を返すコマンド + UI（展開/折り畳み）。
  グループクリックでそのグループ全行に絞り込み。
- 完了条件: グループ件数の総和 = 全件（I5 に検証ケース）。
- 禁止: 単一列の値別パネル（現 `FlagSelect` 相当）との二重実装。既存側は F17 完了後に削除を
  別タスク（T12-1）として切り出す。

#### F18: 一括注釈（対象確定の engine 側実施） → 完了（`2336999`）

- 前提: F3, R5, B3
- 対象: 新規 `src-tauri/src/engine/bulk.rs`
- 手順:
  1. 対象 = `selection(row_indices) | condition(Node) | all` を受け取る `bulk_annotate` コマンド
  2. 実行前に対象件数を返す `bulk_preview` コマンド（UI 確認用）
  3. Undo: 実行前の `(row_index, FlagEntry)` 差分を 1 層だけメモリに保持し戻すコマンド
  4. 対象確定は F2 の評価器で行う（集計・表示と同一述語の保証）
- 完了条件: 「条件一致全件」に一括フラグ → 再読込後も件数・内容が一致（I5）。
- 禁止: フロント側で 1000 行リストを受け取って 1 行ずつ `update_flag` を回す方式。

#### F19: タグ・ブックマーク

- 前提: F18
- 手順: `FlagEntry` に `tags: Vec<String>` と `bookmarked: bool` を追加（serde default で後方互換）。
  `[tag]` のメモ追記方式は維持（互換）。UI はタグ入力 + ブックマークトグル。
- 完了条件: 旧 `flags.json`（tag 無し）の読み込みが壊れない（I5 に旧形式ケース）。

#### F20: 条件付き色

- 前提: F3
- 手順: 条件(F1 の Node) → 色のルール一覧を案件に保存。DataTable のセル描画時に評価。
  評価はフロントで行う（件数はページ単位のため）が、条件モデルは F1 を JSON で共用。
- 完了条件: ルール適用前後でソート・絞り込み結果が不変（I5 + 手動確認 1 回）。

#### F21: プリセット / 保存ビュー

- 前提: F13
- 手順: 条件 + 表示列 + ソートを 1 ファイル（JSON）に書き出し/読み込み。
  案件をまたいで再利用可能に。
- 完了条件: 案件 A で書き出したプリセットを案件 B で適用して件数が変わるが列構成は同じ。

### 取込・精度・出力

#### F22: 区切り文字指定

- 前提: R3
- 手順: `create_project` の payload に `delimiter` を追加（既定は現状の自動）。
  TSV・セミコロン・パイプを許容。
- 完了条件: TSV fixtures で列数が CSV と一致（I5）。

#### F23: UTF-16 対応

- 前提: R3, I1(utf16 variant)
- 手順: BOM 判定で UTF-16LE/BE を検出しデコードしてから取込。
- 完了条件: utf16 fixtures の取込結果が ascii 版と同一（I5 で列数・行数・代表値一致）。

#### F24: JSONL 対応

- 前提: R3
- 手順: 拡張子 `.jsonl` または指定形式で 1 行 1 JSON を取込。列は全キーの和集合。
- 完了条件: ネスト 1 段の JSONL が `a.b` 形式の列に展開される（I5）。

#### F25: 原本 SHA-256 と対応の保持

- 前提: R3
- 手順: 取込時に SHA-256 を計算し `ProjectMeta` に保存。UI は案件詳細に表示のみ。
- 完了条件: I5 で `sha256sum` と一致。

#### F26: 数値・ナノ秒精度

- 前提: F2
- 手順: `anyvalue_to_search_string` の `Float64` 経由をやめ、Int 系は整数文字列化。
  日時はナノ秒を保持した文字列化（`Datetime` の単位を確認してから）。
- 完了条件: `i64::MAX` 相当の値とナノ秒時刻がソート・検索・出力で同一に保たれる（I5）。

#### F27: 出力の条件・列反映

- 前提: F3, R4
- 手順: `export_project` に「全件 / 現在の条件一致 / 表示列のみ / ソート順反映」オプションを追加。
- 完了条件: 条件一致で出力した CSV の行数 = `total_filtered_rows`（I5）。

### 拡張

#### F28: skin の分離（色・記号の外挿）

- 前提: F20
- 手順: フラグ色・記号・条件色のパレットを 1 つの TS モジュールに集約し、
  テーマ（既存 light/dark）と条件色が同じ規則で引けるようにする。
- 完了条件: `npm run lint` + 手動確認 1 回（テーマ切替で条件色も追従）。

## 8. P 層 — 性能（I4 の before に対し after を出す。数値なしなら完了としない）

### P1: ソートキーの事前計算

- 前提: R2, I4
- 対象: `commands/rows.rs:274-313`（ソート比較子。比較のたびに AnyValue→文字列→trim/replace→parse）
- 手順: ソート前に `Vec<(usize, SortKey)>`（列を 1 回変換したキー）を作り、比較はキー同士にする。
- 完了条件: I4 の 200 万行初回ソートが before から改善（数値を docs/performance.md に追記）。

### P2: 検索キャンセルと二重取得の削減（フロント）

- 前提: I5
- 対象: `src/lib/components/ProjectView.svelte`（filterTimeout/filterRequestId 周り）、
  `src/lib/stores/projects.ts`
- 手順: 入力変更で発行済みリクエストを無視（response の requestId 一致のみ採用）。
  同一ページの重複発行を `pendingPages` で排除。Abort は不要（無視で十分）。
- 完了条件: 40 万行 fixtures で検索文字列連続入力時の表示が最終条件のみになることを
  手動確認 1 回 + I5 通過。

### P3: キャッシュ無効化の明文化

- 前提: R5, I4
- 手順: `searchable_cache` / `ioc_flag_cache` の無効条件（IOC 変更時・案件削除時・
  列構成は無関係であること）を `storage.rs` にコメント + テストで固定。
- 完了条件: IOC を 1 件追加した際の再評価が増分のみ（I4 でIOC追加ケースの改善を記録）。

### P4: ページ表示の IOC タグ再計算削減

- 前提: R5
- 対象: `rows.rs:370-437`（ページ行ごとに IOC クエリをトークナイズし直している）
- 手順: R5 の一括評価結果（tags 含む）をキャッシュから読むだけにする。
- 完了条件: I4 の検索切替ケースが改善。

## 9. C 層 — CI・配布

### C1: develop 用 Debian ワークフロー

- 前提: D2（推奨: Tauri 1 継続なら以下でよい）
- 対象: 新規 `.github/workflows/debian.yml`
- 手順:
  1. トリガ: `push: branches: [develop]` + `workflow_dispatch`
  2. `runs-on: ubuntu-24.04`、Node 18、Rust stable
  3. Tauri のシステム依存を導入:
     `sudo apt-get update && sudo apt-get install -y libwebkit2gtk-4.0-dev libgtk-3-dev build-essential curl wget file libxdo-dev libssl-dev`
  4. `npm ci` → `npm run tauri build -- --bundles deb`
  5. アーティファクト: `src-tauri/target/release/bundle/deb/*.deb` + `SHA256SUMS` 生成
  6. `retention-days: 3`
- 完了条件: develop への push でアーティファクトが 3 日保持で上がる（Actions の URL を完了報告に）。
- 禁止: サーバー .deb（サーバー実装が存在しない。X2 まで保留）。100GB は問わない。

### C2: 品質ゲートの組み込み

- 前提: I3, I5
- 手順: `build.yml` と `debian.yml` に `scripts/check_rust_size.sh`、`cargo test`、
  `npm run lint` をビルド前に追加。
- 完了条件: ゲート失敗でワークフローが赤くなる（わざと 501 行ファイルで 1 回確認後、revert）。

## 10. X 層 — 保留（着手条件が明記されたものだけ）

- **X1 DuckDB 再評価**: 着手条件 = X2 の着手判断時（D3）。
- **X2 HTTP API / Python SDK**: 着手条件 = D3 の決定 + F18（bulk）と F14（案件間）の完了。
  サーバー `.deb` もこれに含まれる。
- **X3 Windows 実機検証**: 現環境に Windows 実機が無いため保留。CI の exe 生成は維持。
- **X4 専門解析（プロセス/横移動/永続化/Timestomp/ADS）・ファジー検索**: 非目標に近いが
  IRFlow 差分として残す。着手は F11 の後で別途判断。

## 11. 非目標（タスクにしない）

- Sigma ルール対応。
- 100GB 超への対応・物理ベンチマーク（fixtures は 200 万行まで）。
- 複数端末の同時利用（案件状態のサーバー同期含む）。

## 12. 実行順序

```
D1/D2/D3 は推奨判断を固定済み。異議が無ければこの順に着手する。

I1 → I3 → R0a → I2 → B1 → B3
→ R0b → I5 → I4 → R5 → B2 → R1 → R2 → R3 → R4
→ F1 → F2 → F3 → F4 → F5 → F6        （条件モデル列）
→ F7 → F8 → F9 → F10 → F11           （分析列。F7 が足場）
→ F17 → F18 → F19 → F20 → F21        （注釈列。F18 が足場）
→ F12 → F13 → F14 → F15 → F16        （表示・比較列）
→ F22 → F23 → F24 → F25 → F26 → F27  （取込・出力列）
→ P1..P4（I4 の before に対して随時）
→ C1 → C2
→ 仕上げ: F28、§13 の整理
```

- 列の並びは概ね並行可能（例: F22 〜 F26 は F3 の後ならいつでもよい）。
- 各列の完了ごとに push する（履歴の指示: 「いったん push、develop でも Debian 版
  アーティファクト、保持 3 日」→ C1 がそれを満たす）。
- 100GB と Windows 実機は今後のタスクから外している（履歴の指示どおり）。

## 13. 仕上げタスク（全列の後）

- **Z1 死んだコードの削除**: R0〜R5・F17 の完了後、`grep` で旧経路
  （`build_row_search_text` の単一行用途、`calculate_ioc_applied_records` の旧実装、
  単一列の値別パネル等）を削除。削除理由をコミットメッセージに残す。
- **Z2 README 更新**: Search Syntax 節に条件エディタの追記、Build 節に `scripts/` の説明。
- **Z3 全体チェック**: I5 スモーク + I4 ベンチの再実行、`scripts/check_rust_size.sh`、
  `cargo test`、`npm run lint`、C1 のアーティファクト確認を 1 回通しで実行し、
  結果を docs/validation.md に記録する。

## 14. IRFlow 比較（参照用。タスクは F 層に対応）

| IRFlow 機能 | 現行 0.1.4 | 担当タスク |
|---|---|---|
| 複数条件エディタ（AND/OR 指定・編集） | 無い（部分一致 + 記号Boolean のみ） | F1-F6 |
| 前方/後方一致・正規表現 | 無い | F1/F2 |
| 階層グルーピング（複数列・展開・件数） | 単一列の値別パネルのみ | F17 |
| 一括注釈（選択/一致全件/全データ） | 単行のみ | F18 |
| 任意タグ・独立ブックマーク | `[tag]` メモ追記のみ | F19 |
| 条件付き色 | スキンとフラグ色のみ | F20/F28 |
| プリセット（別ファイル再利用） | 無い | F21 |
| 列統計・希少値 | 無い | F7/F8 |
| 重複/単独値抽出 | 無い | F9 |
| 時間範囲・ヒストグラム・ギャップ/セッション/バースト | 無い | F10/F11 |
| 周辺イベント・隠し列を含む詳細 | 無い | F12 |
| 戻る/進む・状態復元 | 無い | F13 |
| 案件間検索・差分（重複件数保持） | 無い | F14/F15 |
| HTML レポート | 無い | F16 |
| UTF-16・区切り文字・JSONL・SHA | 無い | F22-F25 |
| 数値/ナノ秒精度 | f64 経由で丸まりうる | F26 |
| 出力の条件・列反映 | 全件固定 | F27 |
| 専門解析・ファジー検索 | 無い | X4（保留） |

## 15. 運用

- `develop` を既定開発ブランチとする。`main` は旧 0.1.4 のまま。旧版を前回の成果として push しない。
- 履歴報告（旧 develop の機能・計測・テスト数）を完了根拠にしない。
- 各カードの完了報告は「実行コマンドと結果」のみ。判断が必要になったら D 層に切り出し、
  実行者が独断で決めない。
