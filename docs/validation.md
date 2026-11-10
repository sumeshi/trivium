# Validation（Z3 全体チェック）

- 実施日: 2026-11-10
- 環境: WSL2 (Linux 6.18.58.1-microsoft-standard-WSL2)、Intel Core i5-12400F、メモリ 15GiB
- 対象コミット: `develop` ブランチ、Z2 完了時点（`0d63591`）

## 実行コマンドと結果

| チェック | コマンド | 結果 |
|---|---|---|
| I5 往復スモーク | `cargo test -p trivium-engine --test smoke` | 20 passed / 0 failed |
| 全テスト | `cargo test -p trivium-engine` | 23 スイート / 97 テスト / 0 failed |
| Rust 規模ガード | `bash scripts/check_rust_size.sh` | exit 0（500 行超過なし） |
| フロント Lint | `npm run lint` | svelte-check 0 errors / 0 warnings |
| I4 ベンチ再実行 | `cargo run --release -p trivium-engine --bin bench -- fixtures/bench-100k.csv fixtures/bench-400k.csv fixtures/bench-2m.csv` | 下表 |
| C1 deb アーティファクト | develop への push + Actions 確認 | **未確認**（push 保留中。push 後に確認する事項） |

## ベンチ再実行（P1/P3 適用後の現行値）

| 項目 | 100k | 400k | 2M |
|---|---:|---:|---:|
| create | 65ms | 231ms | 1.06s |
| sort_first | 41ms | 105ms | 534ms |
| export_all | 17ms | 67ms | 404ms |
| search_A_cold | 254ms | 1.32s | 7.45s |
| search_B_cold | 159ms | 630ms | 3.44s |
| search_A_warm | 144ms | 585ms | 3.14s |
| search_B_warm | 157ms | 666ms | 3.34s |
| ioc_100_save | 1.30s | 5.09s | 26.8s |
| ioc_100_query | 74ms | 289ms | 1.86s |
| ioc_1_more_save | 113ms | 434ms | 2.42s |
| ioc_1_more_query | 91ms | 381ms | 1.75s |
| flag_update | 16ms | 28ms | 109ms |

- ベースライン（P1/P3 適用前）との比較は `docs/performance.md` を参照。
- `flag_update`（400k）は B3 の完了条件 100ms 以内を満たす（28ms）。
- `ioc_1_more_*` は P3 の増分評価により、全再評価のベースライン比で大幅短縮
  （2M: save 25.1s → 2.42s、query 27.1s → 1.75s）。
- `ioc_100_save` は初回構築のため全評価（増分対象外。追加のみ増分）。
- 計測は 1 実行 1 回。数値の良し悪しの判断はしない。

## 残った確認事項（push 後に実施）

1. C1: develop push → `debian.yml` の deb アーティファクト確認（保持 3 日）。
2. C2: ゲート失敗時にワークフローが赤くなることの確認
   （わざと 501 行ファイルを置いて 1 回確認後 revert）。
3. GUI 手動確認（F4-F6、F8、F10-F13、F28 等の「GUI 操作は未確認」タスク）。
   ローカルでは webkit2gtk がビルドできないため未実施のまま。
