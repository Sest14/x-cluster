# x-cluster

X 上の言説と、その発言をしているアカウントを、集団レベルでクラスタ分析するプロジェクト。

- 方法論（必要な知識、統計の出し方、統計のマジック対策）：[docs/methodology.md](docs/methodology.md)
- 分析計画書（事前登録）：[docs/analysis-plan.md](docs/analysis-plan.md)
- 意思決定の記録（ADR）：[docs/adr/](docs/adr/)

---

## 収集スクリプト（xc.py）

投稿とプロフィールを TwexAPI で集め、公式 X API で網羅性を検査して、このフォルダ内の SQLite に保存します。分析用に仮名化したファイル（analysis.sqlite）を書き出すところまでを担当します（ADR-0014）。

## 必要なもの
- Python 3.9 以上（標準ライブラリのみで動くので、追加インストールは不要）
- このフォルダの `.env` に次の2行
  ```
  X_BEARER_TOKEN=公式 X API の Bearer Token
  TWEX_API_KEY=TwexAPI の APIキー
  ```

## 実行順
コマンドプロンプトでこのフォルダに移動してから実行します。ふだんは次の2つだけで足ります。
```
py xc.py tcheck      … TwexAPI のキーと時間指定の書き方を確認（数百クレジット）
py xc.py run         … 収集一式（count → tsearch → sample → tbaseline → tany → weights → coverage → export）
```

個別に実行する場合はこちら。
```
py xc.py check       … 公式APIのトークン確認（約$0.005）
py xc.py count       … 公式API：検索条件ごとの日別件数（網羅性の基準）
py xc.py tsearch     … TwexAPI：起点の投稿を集める（6件は全件、touken_fans と対照群は日別比例の抽出）
py xc.py sample      … 起点の投稿者から各群のアカウントを無作為抽出
py xc.py tbaseline   … TwexAPI：抽出したアカウントの炎上前の投稿（なければ直近の投稿）
py xc.py tany        … TwexAPI：touken_fans と対照群が炎上に言及したか（H3）
py xc.py weights     … 公式API：touken_fans と対照群の重み用の件数
py xc.py coverage    … 公式API：TwexAPI の網羅性の抜き取り照合
py xc.py export      … analysis.sqlite（仮名化済み）を作成
py xc.py status      … 現在の件数、使用クレジット、公式APIの推定使用額
```

どのコマンドも途中で止めて（Ctrl+C）、同じコマンドを再実行すれば続きから再開します。
TwexAPI の使用クレジットが `twex_max_credits`、公式APIの推定使用額が `budget_usd` を超えそうになると自動で止まります。

## 設定（config.json）
- `queries`：検索条件。`name` はラベル、`query` は公式APIの検索演算子で書く（TwexAPI 用には自動で書き換える）。`collect` は `all`（全件）か `sample`（日別比例で `max_seed_posts` 件）。`n_accounts` は抽出する人数
- `start_time` / `end_time`：調査期間（`end_time` が `null` なら収集時点まで）
- `baseline_start` / `baseline_end`：「ふだん」を測る炎上前の期間。`baseline_max_posts` は1人あたりの取得上限、`recent_max_posts` は炎上前の投稿がない人の取得上限
- `daily_words`：日常の話を見分けるための語
- `phase1_rule`：運用の型の判定ルール（事前登録。分析を始めたら変えない）
- `label_sample_per_group`：Claude が詳しく判定するアカウントの数（各群）
- `any_controversy_query` / `any_controversy_groups`：H3 の言及確認に使う検索条件と対象の群
- `twex_qps`：TwexAPI への1秒あたりのリクエスト数（プランに合わせる。無料枠は0.2、Mini は1）
- `twex_time_format`：時間の範囲の書き方（`unix` か `date_utc`。`tcheck` の結果で選ぶ）
- `twex_max_credits`：TwexAPI のクレジット使用の上限
- `coverage_windows_per_query` / `coverage_posts_per_window`：網羅性の照合の規模
- `budget_usd`：公式APIの推定使用額の上限

## ファイル
- `raw.sqlite`：取得したそのままのデータ。**このPCの外に出さないでください**
- `salt.secret`：仮名化に使う秘密鍵。**このPCの外に出さないでください**
- `analysis.sqlite`：アカウントID・@名・URLを仮名化したデータ。分析にはこれを使います

## データの保持
TwexAPI のデータは、X 上での削除への追従ができません。分析が終わったら `py xc.py purge --yes` で生データを消してください。集計結果は残ります。
