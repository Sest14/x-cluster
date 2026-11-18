# x-cluster

X 上の言説と、その発言をしているアカウントを、集団レベルでクラスタ分析するプロジェクト。

- 方法論（必要な知識、統計の出し方、統計のマジック対策）：[docs/methodology.md](docs/methodology.md)
- 分析計画書（事前登録）：[docs/analysis-plan.md](docs/analysis-plan.md)
- 意思決定の記録（ADR）：[docs/adr/](docs/adr/)

---

## 収集スクリプト（xc.py）

X API（従量課金）で投稿を集めて、このフォルダ内の SQLite に保存します。分析用に仮名化したファイル（analysis.sqlite）を書き出すところまでを担当します。

## 必要なもの
- Python 3.9 以上（標準ライブラリのみで動くので、追加インストールは不要）
- このフォルダの `.env` に `X_BEARER_TOKEN=...`

## 実行順
コマンドプロンプトでこのフォルダに移動してから実行します。

ふだんは次の2つだけで足ります。
```
py xc.py count       … 検索条件ごとの該当件数と費用の見積もり
py xc.py run         … 収集一式（search → users → timelines → export）
```

個別に実行する場合はこちら。
```
py xc.py check       … トークン確認（約$0.005）
py xc.py count       … 検索条件ごとの該当件数を確認（1条件あたり約$0.005）
py xc.py search      … 起点となる投稿を収集
py xc.py users       … 投稿者を無作為抽出し、プロフィールを取得
py xc.py timelines   … 抽出したアカウントの直近の投稿を取得
py xc.py export      … analysis.sqlite（仮名化済み）を作成
py xc.py status      … 現在の件数と推定使用額
```

どのコマンドも途中で止めて（Ctrl+C）、同じコマンドを再実行すれば続きから再開します。
推定使用額が config.json の `budget_usd` を超えそうになると自動で止まります。正確な請求額は Developer Console で確認してください。

## 設定（config.json）
- `queries`：検索条件。`name` はラベル、`query` は X の検索演算子（`OR`、`-is:retweet`、`lang:ja` など）
- `search_endpoint`：`recent`（直近7日）または `all`（全期間。使える場合のみ）
- `start_time` / `end_time`：期間の指定（例 `"2026-11-01T00:00:00Z"`）。指定しないときは `null`
- `max_seed_posts_per_query`：1つの条件あたりの収集上限
- `max_accounts`：分析対象として無作為抽出するアカウント数（検索条件ごとに均等に割り当て）
- `timeline_posts_per_account`：1アカウントあたりの取得件数
- `budget_usd`：推定使用額の上限

## ファイル
- `raw.sqlite`：X から取得したそのままのデータ。**このPCの外に出さないでください**
- `salt.secret`：仮名化に使う秘密鍵。**このPCの外に出さないでください**
- `analysis.sqlite`：アカウントID・@名・URLを仮名化したデータ。分析にはこれを使います

## 規約の順守（削除への追従）
X の規約では、X 上で削除・編集・非公開化された投稿は手元でも削除・修正する必要があります。
- 分析が終わったら `py xc.py purge --yes` で生データを消すのが一番確実です。集計結果は残ります。
- データを長く保持する場合は、定期的に `py xc.py sync --yes` を実行してください。照合のたびに件数に応じた費用がかかります。そのあと `py xc.py export` で analysis.sqlite を作り直します。
