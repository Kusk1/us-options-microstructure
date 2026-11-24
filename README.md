# 美股期权微观结构与流动性踩踏监控仪表盘（OpenBB 自动版）

基于 **OpenBB Platform（`yfinance`）+ CBOE 官方公开数据**，零 API Key、纯免费运行的每日自动化美股期权微观结构监控仪表盘。支持 **GitHub Actions 每日美股收盘后自动更新**并发布到 **GitHub Pages**，同时保留完整的交互式压力测试滑块、情景推演、每日历史走势沉淀与逐指标来源/缺口审计。

---

## 目录结构

```text
.
├── .github/workflows/daily-update.yml  # GitHub Actions 定时任务（美股收盘后每日自动抓数 + 部署 Pages）
├── pyproject.toml                      # Python 3.12 + OpenBB 4.7.2 依赖声明（供云端 uv sync 使用）
├── scripts/
│   └── fetch_data.py                   # 数据采集、SPX 期权链 Black-Scholes 计算与历史沉淀脚本
└── docs/                               # GitHub Pages 静态发布目录
    ├── index.html                      # 仪表盘前端（Chart.js + Tailwind CSS）
    └── data/
        ├── latest.json                 # 最新一期快照、口径来源审计与 SPX 分行权价 Gamma 墙
        └── history.json                # 每日历史时间序列（首次自动回填近 90 个交易日）
```

---

## 14 项监控指标的数据来源与口径说明

| 维度 | 指标 | 数据来源 | 口径分类 | 说明 |
| :--- | :--- | :--- | :--- | :--- |
| **一、离散与相关性** | `VIXEQ / VIX` | CBOE `VIXEQ_History.csv` ÷ OpenBB `^VIX` | 派生 | 成分股隐波 ÷ 指数隐波 |
| | `DSPX` | CBOE `DSPX_History.csv` | 官方 | Cboe S&P 500 30日离散度指数 |
| | `NVDA` | OpenBB `NVDA` (`yfinance`) | 官方 | 权重科技龙头风向标收盘价 |
| **二、波动率与挤兑** | `VIX` (30D) | OpenBB `^VIX`（CBOE CSV 兜底） | 官方 | 30天基准恐慌指数 |
| | `VVIX` | OpenBB `^VVIX`（CBOE CSV 兜底） | 官方 | 波动率的波动率 |
| **三、期限与偏度** | `VIX / VXV` | OpenBB `^VIX` ÷ `^VIX3M` | 派生 | 近远端倒挂比率（`> 1.00` 为倒挂） |
| | `VIX9D / VIX` | OpenBB `^VIX9D` ÷ `^VIX` | 派生 | 超短端爆发斜率（`> 1.00` 为倒挂） |
| | `CBOE SKEW` | OpenBB `^SKEW`（CBOE CSV 兜底） | 官方 | 标普尾部风险偏度指数 |
| **四、期权流量** | 个股 / 全市场 / 指数 `P/C` | CBOE Daily Market Statistics JSON | 官方 | 每日官方成交量 Put/Call 比率 |
| **五、做市商敞口** | `SPX GEX` | OpenBB `^SPX` 全量期权链 → Black-Scholes | 期权链自算 | 单位 `$B / 1% 变动`（标准做市商假设：Call `+OI`、Put `-OI`） |
| | `SPX Dealer Agg Gamma` | OpenBB `^SPX` 全量期权链 → Black-Scholes | 期权链自算 | 单位 `$B / 10 点变动` |
| | `Estimated Net Delta Flow` | OpenBB `^SPX` 全量期权链 → 成交量加权 Delta | 期权链自算 | 单位 `$B 名义流`（当日成交量 × Delta × 100 × Spot） |

> **口径透明提醒**：
> 1. 做市商敞口三项采用免费公开期权链按标准做市商库存假设自算，**零轴翻转方向与相对压力趋势可靠**，但与 SpotGamma / SqueezeMetrics 等基于真实客户订单流的付费模型绝对数值存在口径差异。前端滑块支持随时手动输入外部数值覆盖推演。
> 2. 历史回填（近 90 个交易日）包含全部官方指数与 CBOE 官方 Put/Call；过去日期的做市商敞口在免费源中不可回溯，因此历史行中如实留空（`null`，绝不补 `0`），并从每日定时任务启用之日起逐日累积真实自算值。

---

## 本地运行与预览（直接复用 `/Volumes/Data/openbb/.venv`）

本机无需重复安装 OpenBB，直接调用现有的 `/Volumes/Data/openbb/.venv/bin/python` 即可：

```bash
# 1. 手动触发一次最新数据抓取（增量更新默认回看近 10 天；首次可指定 90 天）
/Volumes/Data/openbb/.venv/bin/python scripts/fetch_data.py --backfill-days 90

# 2. 启动本地静态服务器预览网页
python3 -m http.server 8080 --directory docs
# 浏览器打开 http://127.0.0.1:8080/
```

---

## 一键发布到 GitHub Pages + 每日云端自动更新

只需 4 步即可免费上线（手机、电脑随时打开，每天北京时间 06:30 自动刷新）：

1. **在 GitHub 新建一个仓库**（建议 Public 仓库，可免费无限使用 GitHub Pages 与 GitHub Actions；比如命名 `us-options-microstructure`）。
2. **在当前目录初始化 Git 并推送代码**：
   ```bash
   git init
   git branch -M main
   git add .
   git commit -m "feat: initial OpenBB options microstructure monitor"
   git remote add origin https://github.com/<你的GitHub用户名>/<仓库名>.git
   git push -u origin main
   ```
3. **开启 GitHub Pages（使用 GitHub Actions 部署）**：
   - 打开仓库页面 → **Settings** → **Pages** → **Build and deployment** → Source 选择 **`GitHub Actions`**。
4. **开启定时任务与首次运行**：
   - 打开仓库页面 → **Actions** → 选择 **`Daily OpenBB Options Microstructure Update`** → 点击 **`Run workflow`** 手动触发一次，等待约 2~3 分钟完成后，即可通过 `https://<你的GitHub用户名>.github.io/<仓库名>/` 访问！
   - 之后每个美股交易日收盘后（UTC 22:30，北京时间次日 06:30），GitHub Actions 会自动拉取 OpenBB + CBOE 数据、更新 `docs/data/*.json` 并重新发布页面。
