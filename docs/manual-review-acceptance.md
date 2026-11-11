# Manual review acceptance — T26

- 記録日: 2026-11-11
- 対象: `GOAL.md` T26「実アプリで手動調査の一周を確認する」
- 状態: **未完了（実機確認待ち）**
- 自動回帰: `crates/engine/tests/manual_loop_test.rs`（6 テスト）

T26 の完了条件は「基本の一周を実アプリで通せる」ことと、T20d の
2M 行の先頭・中間・末尾へ実 WebView で到達できること。この環境では
Tauri ネイティブアプリをビルド・起動できないため、T26 は完了にしない。
以下の表は、自動テストで確認できた範囲と、実機でしか確認できない範囲を
分けて記録する。

## 状態の区分

| 区分 | 意味 |
|---|---|
| **自動テストで確認済み** | `cargo test -p trivium-engine` / `npm run test:ui` で同じ契約を検証済み。実画面の操作は含まない。 |
| **モック・コードレビューで確認** | UI の配線、状態遷移、純粋関数をコードと既存の UI テストで確認済み。ネイティブウィンドウでの操作は未実施。 |
| **実 native アプリで未確認** | Tauri アプリを起動しての確認がまだ。T26 の完了条件に対して未達。 |

## 実行環境の制約（この記録を作成した環境）

- GTK / WebKitGTK の開発ライブラリがない。この環境では `pkg-config --exists
  gdk-3.0` と `pkg-config --exists webkit2gtk-4.1` が失敗し、
  `cargo check -p trivium-desktop` はネイティブ依存のビルドで停止する。
- `sudo` はパスワードを要求し、非対話では使用できない。不足パッケージを
  インストールできない。
- Tauri アプリはビルド・起動していない。GUI セッションの有無にかかわらず、
  この環境で実ウィンドウを操作した確認はない。
- したがって、ファイルダイアログ、クリップボード、WebView のスクロール、
  プロセス再起動を含む確認は実施していない。ブラウザのモックだけを
  「Tauri アプリで確認済み」とは扱わない。

## 実機確認に必要な手順

1. `develop` に対する Debian build（`.github/workflows/debian.yml`）を
   push または `workflow_dispatch` で実行する。この workflow は
   `libwebkit2gtk-4.1-dev`、`libgtk-3-dev` などを導入し、
   `npm run tauri build -- --bundles deb` を実行する。
2. Actions の artifact `trivium-deb` から `.deb` と `SHA256SUMS` を取得する
   （保持期間は 3 日）。`SHA256SUMS` で検証する。
3. Kali などの Debian 系デスクトップで、artifact の実際のファイル名を使って
   インストールする（例: `sudo apt install ./Trivium_0.1.4_amd64.deb`）。
   アプリケーションメニューの「Trivium」から起動する。
4. 2M 行 fixture を作る:
   `python3 scripts/gen_fixture.py --rows 2000000 --seed 42 --out fixtures/bench-2m.csv`
5. 下のチェックリストを上から実行し、各項目の結果をこのファイルへ追記する。
   失敗または未確認の項目は、その担当カードを未完了へ戻す。
6. 終了・再起動・案件切替を挟む項目は、アプリを完全に終了してから再起動して
   確認する。プロセスを残したままの再読込では代用しない。

## 受け入れチェックリスト

### 1. 入力と表示

| # | 項目 | 期待 | 状態 | 根拠 / 備考 |
|---|---|---|---|---|
| 1.1 | CSV を実 UI のファイル選択から開く | 3 行が表示され、案件が作成される | **実 native アプリで未確認** | エンジンの CSV 取込は自動テスト済み。ダイアログと画面は未操作。 |
| 1.2 | JSON 配列を実 UI から開く | 配列全体でなく 3 行になる | **実 native アプリで未確認** | `json_import_test` / `manual_loop_test` で engine は確認済み。 |
| 1.3 | JSONL（NDJSON）を実 UI から開く | 3 レコードが 3 行になる | **実 native アプリで未確認** | 同上。拡張子の大文字小文字も engine テスト済み。 |
| 1.4 | CSV の表示値 | 先頭ゼロ、カンマ、引用符、セル内改行、長整数、空白、入力由来 `flag`/`memo`/`__rowid`/`trivium-memo` が元文字列のまま | **自動テストで確認済み** | `manual_loop_test::csv_display_and_export_keep_the_t00_source_strings`。実画面の目視は未実施。 |
| 1.5 | JSON / JSONL の表示値 | i64::MAX、u64::MAX、2^53+1、負の i64::MIN、配列、`a.b` と `a`→`b` の別列、null / 欠損 / 空文字の区別 | **自動テストで確認済み** | `manual_loop_test::json_and_jsonl_display_the_same_exact_values`。 |
| 1.6 | 大量行の末尾スクロール | 末尾まで空白の長い表が残らず、最後のページに重複・欠落がない | **モック・コードレビューで確認** | `virtualScroll.test.ts` の最終ウィンドウ検証と T07 の clamp 実装。実 WebView は未確認。 |
| 1.7 | 2M 行の先頭・中間・末尾への到達 | 1 行目、1,000,000 行目、2,000,000 行目へ移動でき、最後のページが正しい | **実 native アプリで未確認** | 純粋関数の写像テスト（`virtualScroll.test.ts`）のみ。実 WebView での T20d 確認は未実施。 |

### 2. 検索・絞り込み・並べ替え

| # | 項目 | 期待 | 状態 | 根拠 / 備考 |
|---|---|---|---|---|
| 2.1 | 全文検索 | 入力した語で結果が絞られ、件数が一致する | **自動テストで確認済み** | 検索評価は既存テストと `manual_loop_test` の列スコープ検索で確認。実入力欄の操作は 2.5 と未確認一覧に記載。 |
| 2.2 | 非表示列を条件に使う | 非表示にした列の条件・列指定検索がヒットする | **自動テストで確認済み** | `manual_loop_test::hidden_column_condition_and_search_match_without_returning_the_column`。 |
| 2.3 | 非表示列がページ応答に混ざらない | `rows[].data` と `rows[].cells` に非表示列がない | **自動テストで確認済み** | 同上（T20a の契約）。 |
| 2.4 | ソート | 指定列の昇順・降順で表示とページ順が一致する | **自動テストで確認済み** | `manual_loop_test::sorted_pages_selection_bulk_and_selected_export_agree` と既存の sort テスト。 |
| 2.5 | 検索の連続入力 | 古い検索の応答が新しい検索の行・件数・pending を変えない | **モック・コードレビューで確認** | T05 の世代管理。実機で遅延応答の再現は未実施。 |
| 2.6 | 検索後の末尾位置 | 結果件数が減ったときスクロール位置が範囲内へ戻る | **モック・コードレビューで確認** | T07 の clamp と `virtualScroll` の範囲検証。実 WebView は未確認。 |

### 3. 詳細・注釈・選択

| # | 項目 | 期待 | 状態 | 根拠 / 備考 |
|---|---|---|---|---|
| 3.1 | 詳細を開いて全文を読む | 長いセルは preview ではなく全文を取得して表示する | **モック・コードレビューで確認** | `getCell`、T20c のエンジンテスト、`ExpandedCellDialog` の実装。実ダイアログは未操作。 |
| 3.2 | 詳細からコピー | 表示中の全文と同一の文字列をコピーする | **モック・コードレビューで確認** | `ipcValues.test.ts` の `copyText`。OS クリップボードへの実コピーは未確認。 |
| 3.3 | 長整数のコピー値 | `9007199254740993` などが丸め・指数表記なしで文字列のまま | **自動テストで確認済み** | `manual_loop_test::ipc_json_sends_long_integers_as_plain_decimal_strings` と `ipcValues.test.ts`。クリップボードは未確認。 |
| 3.4 | フラグ・メモ・タグ・ブックマークを付ける | 4 つが独立して保存され、解除しても他を消さない | **自動テストで確認済み** | `manual_loop_test::annotations_survive_reopen_and_bookmark_only_is_empty_at_zero`、T02/T06/T13 のテスト。実クリックは未確認一覧に記載。 |
| 3.5 | 選択行への一括注釈 | ページをまたいだ選択 ID だけが更新される | **自動テストで確認済み** | `manual_loop_test` と `annotation_tags_test` が元行 ID への一括更新を確認。実チェックボックス操作は未確認一覧に記載。 |
| 3.6 | 選択件数・全件・結果件数の区別 | ラベルと件数が一致し、0 件選択で全件へフォールバックしない | **モック・コードレビューで確認** | T22/T23 の UI と engine の空選択エラー。実画面は未確認。 |
| 3.7 | ブックマークのみで 0 件 | `total_filtered_rows == 0`、行が空、空状態が出る | **自動テストで確認済み** | `manual_loop_test` が engine の 0 件を確認。空状態の描画は T07 のコードレビュー範囲で、実画面は未確認一覧に記載。 |
| 3.8 | 保存失敗 | 失敗が画面に出て、変更後の表示が残らず、再試行できる | **モック・コードレビューで確認** | T06 の実装。失敗注入は実機で未実施。 |

### 4. 出力・終了・再開

| # | 項目 | 期待 | 状態 | 根拠 / 備考 |
|---|---|---|---|---|
| 4.1 | 現在の結果を出力 | 表と同じ元行 ID・件数・順序で CSV が出る | **自動テストで確認済み** | `manual_loop_test` の selected 出力と T23 のテスト。実保存ダイアログは未確認一覧に記載。 |
| 4.2 | 選択行を出力 | 選択した行だけが結果順で出る。0 件なら全件へ落ちない | **自動テストで確認済み** | `manual_loop_test` と `export_scope_test`。 |
| 4.3 | 出力の表示値・コピー値・出力値の一致 | 同じ元文字列になる | **自動テストで確認済み** | `manual_loop_test` が CSV の表示と出力を全列で比較。コピーは `ipcValues` の同一文字列契約。 |
| 4.4 | 出力の注釈列 | flag/memo/tags/bookmark が独立した列で出る | **自動テストで確認済み** | `manual_loop_test` と `export_scope_test`。 |
| 4.5 | 案件切替 | 旧案件の行・件数・履歴・ダイアログが新案件へ漏れない | **モック・コードレビューで確認** | T05/T24 の世代管理と復元処理。実機の A→B 切替は未確認。 |
| 4.6 | 終了・再開 | 注釈とビュー状態が残り、再起動後に同じ調査を再開できる | **自動テストで確認済み** | `manual_loop_test` が `ProjectsStore` 再作成後も注釈を確認。T24 は view state を engine テスト済み。実プロセス再起動は未確認一覧に記載。 |
| 4.7 | ダイアログの Cancel | 出力・メモ編集を取り消し、保存しない | **モック・コードレビューで確認** | `ExportDialog` / `MemoEditorDialog` の実装。実操作は未確認。 |
| 4.8 | 実行中ジョブの Cancel | 検索・取込・出力を中断し、古いジョブが新しい状態を変えない | **自動テストで確認済み** | T15–T18/T23 の job テストが中断要求と旧ジョブの非干渉を確認。実行中ジョブを止める UI ボタンは現行画面に配線されていない（コードレビュー）。実機操作は未確認一覧に記載。 |

## 実機で未確認の項目一覧

T26 の完了を妨げる未確認項目:

1. CSV / JSON / JSONL を実 UI のファイル選択から開く（1.1–1.3）。
2. 実 WebView での 2M 行の先頭・中間・末尾への到達と最終ページ（1.7、T20d）。
3. 実 WebView での末尾スクロール、絞り込み後の位置補正（1.6、2.6）。
4. 検索入力欄での連続入力と、遅い旧応答の破棄（2.5）。
5. 詳細ダイアログの全文表示・OS クリップボードへのコピー（3.1、3.2）。
6. 実際のクリックによるフラグ・メモ・タグ・ブックマーク保存と失敗表示（3.4、3.8）。
7. チェックボックスによる選択、一括注釈、件数表示（3.5、3.6）。
8. 出力ダイアログ、保存先選択、成功・警告通知（4.1、4.4）。
9. アプリの完全終了と再起動、案件切替、ビュー状態の復元（4.5、4.6）。
10. ダイアログの Cancel と、実行中ジョブの Cancel 操作（4.7、4.8）。

失敗はこの記録の時点で観測していない。未確認の項目は「確認済み」ではなく、
担当カード（T05、T06、T07、T11、T13、T20b–T20d、T21–T25）の実機確認として
未完了に戻す。

## 自動回帰テストの対応

`crates/engine/tests/manual_loop_test.rs`（6 テスト）:

| テスト | T26 の確認内容 |
|---|---|
| `csv_display_and_export_keep_the_t00_source_strings` | CSV の表示値と出力値が T00 fixture の元文字列と一致する（長整数・先頭ゼロ・カンマ・引用符・セル内改行・空白・入力由来の予約名風列）。 |
| `json_and_jsonl_display_the_same_exact_values` | JSON / JSONL の長整数、配列、`a.b` と `a`→`b`、null / 欠損 / 空文字。 |
| `hidden_column_condition_and_search_match_without_returning_the_column` | 非表示列の条件と列指定検索がヒットし、ページ応答に非表示列が含まれない（T20a）。 |
| `sorted_pages_selection_bulk_and_selected_export_agree` | ソート、2 ページ、選択 ID の一括注釈（T22）、selected 出力の件数・順序・注釈列（T23）。 |
| `annotations_survive_reopen_and_bookmark_only_is_empty_at_zero` | 4 種の注釈保存 → `ProjectsStore` 再作成 → 復元。bookmark-only 0 件と、bookmark だけの解除。 |
| `ipc_json_sends_long_integers_as_plain_decimal_strings` | `serde_json::to_value` 後のセル text が指数表記・丸めのない 10 進文字列（T10 の再確認）。 |

既存の UI テスト（`npm run test:ui`）は、IPC 値の表示・コピー、ページキャッシュ、
仮想スクロール、選択、条件追加、ビュー状態写像、パネルの遅延要求を検証する。
これらは実 WebView の操作ではない。
