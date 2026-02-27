# Kibana Dashboard Setup

## Quick Start

### Option 1: Auto-setup (recommended)

```bash
chmod +x kibana/import_saved_objects.sh
./kibana/import_saved_objects.sh
```

### Option 2: Manual Setup

1. Go to http://localhost:5601
2. **Stack Management** → **Index Patterns**
3. Create:
   - `nba_team_metrics` (time field: `game_date`)
   - `nba_match_metrics` (time field: `game_date`)

---

## Recommended Visualizations

### nba_team_metrics Index

| # | Visualization | Type | Description |
|---|---------------|------|-------------|
| 1 | **Win Rate by Team** | Bar Chart | Avg win_rate_last5 by team_name |
| 2 | **Avg Points Last 5** | Bar Chart | Avg points scored (last 5 games) per team |
| 3 | **Home vs Away Performance** | Pie Chart | Wins distribution (home=1, away=0) |
| 4 | **Win Rate by Conference** | Bar Chart | East vs West comparison |
| 5 | **Rest Days Impact** | Line Chart | Win rate by rest_days |
| 6 | **Team Rankings Table** | Data Table | Teams ranked by multiple KPIs |

> **Note**: Schedule Difficulty (SoS) visualizations are not yet functional — the `schedule_difficulty_next5` field is currently a constant placeholder (1.0). See project perspectives for planned implementation.

### nba_match_metrics Index

| # | Visualization | Type | Description |
|---|---------------|------|-------------|
| 1 | **Win Probability Distribution** | Histogram | Distribution of predicted probabilities |
| 2 | **Prediction Accuracy** | Gauge | % correct predictions (if home_win matches prob > 0.5) |
| 3 | **Home Win Rate Over Time** | Line Chart | Trend of home wins per month |
| 4 | **Score Difference Distribution** | Histogram | home_score - visitor_score |
| 5 | **Recent Form vs Result** | Heatmap | Win rate (last 5) vs actual result |
| 6 | **Prediction Confidence** | Gauge | Avg confidence (prob closest to 0.5 = uncertain) |

---

## Filters (Global)

Add these to your dashboard:

- **Season**: Filter by `season` (2022, 2023, 2024, 2025)
- **Team**: Filter by `team_name` (dropdown)
- **Date Range**: Use `game_date` (default: last 30 days)

---

## Dashboard Layout

```
┌─────────────────────────────────────────────────────────────┐
│  Filters: [Season ▼] [Team ▼] [Date Range]                  │
├─────────────────────────────────────────────────────────────┤
│  Win Rate by Team (Top 10)                                  │
│  ┌─────────────────────────────────────────────────────────┐│
│  │ ████████████████████████████                            ││
│  └─────────────────────────────────────────────────────────┘│
├─────────────────────────────┬───────────────────────────────┤
│  Avg Points Last 5          │  Home vs Away Performance     │
│  ┌───────────┐              │  ┌───────┐                    │
│  │ ████      │              │  │ 60%   │                    │
│  └───────────┘              │  │ 40%   │                    │
│                             │  └───────┘                    │
├─────────────────────────────┴───────────────────────────────┤
│  Win Probability Distribution                               │
│  ┌─────────────────────────────────────────────────────────┐│
│  │ 0.0  0.2  0.4  0.6  0.8  1.0                            ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

---

## Fields Reference

### nba_team_metrics

- `team_name` - Team full name
- `team_id` - Team ID
- `conference` - East/West
- `division` - Division name
- `game_id` - Game ID
- `game_date` - Date of game
- `season` - NBA season year
- `is_home` - 1 = home, 0 = away
- `is_win` - 1 = win, 0 = loss
- `points_for` - Points scored
- `points_against` - Points conceded
- `schedule_difficulty_next5` - Schedule difficulty (placeholder, constant 1.0)
- `home_games_next5` - Home games next 5 (placeholder)
- `away_games_next5` - Away games next 5 (placeholder)

### nba_match_metrics (29 fields)

- `game_id` - Game ID
- `game_date` - Date of match
- `home_team_id` / `visitor_team_id` - Team IDs
- `home_team_name` / `away_team_name` - Team names
- `home_team_score` / `visitor_team_score` - Final scores
- `home_win` - 1 = home win, 0 = away win
- `win_probability_home` - ML prediction (0-1)
- `prediction` - Binary prediction (0 or 1)
- `home_win_rate` / `away_win_rate` - Cumulative win rates
- `home_avg_points` / `away_avg_points` - Avg points scored
- `home_avg_points_against` / `away_avg_points_against` - Avg points conceded
- `home_win_rate_last5` / `away_win_rate_last5` - Recent form (last 5)
- `home_avg_points_last5` / `away_avg_points_last5` - Recent avg points
- `home_rest_days` / `away_rest_days` - Rest days before game
- `win_rate_diff` / `avg_points_diff` / `avg_points_against_diff` - Differential features
- `rest_days_diff` / `form_last5_diff` / `home_advantage` - Context features

---

## How to Create Advanced Visualizations

### Heatmap: Recent Form vs Result

1. Create new Visualization → **Heatmap**
2. Select `nba_match_metrics` index
3. Y-Axis: `win_probability_home` (bucket: range 0-0.2, 0.2-0.4, 0.4-0.6, 0.6-0.8, 0.8-1.0)
4. Color: Avg of `home_win`
5. This shows: predicted probability bucket → actual win rate (diagonal = good predictions)

### Gauge: Prediction Accuracy

1. Create new Visualization → **Gauge**
2. Select `nba_match_metrics` index
3. Metric: Avg of script `doc['home_win'].value > 0.5 == doc['win_probability_home'].value > 0.5 ? 1 : 0`
   - Or simpler: Count where `(home_win = 1 AND win_probability_home > 0.5) OR (home_win = 0 AND win_probability_home < 0.5)`
4. Format: Percentage

### Gauge: Prediction Confidence

1. Create new Visualization → **Gauge**
2. Select `nba_match_metrics` index
3. Metric: Avg of `abs(win_probability_home - 0.5) * 2`
4. This gives 0% = completely uncertain, 100% = very confident
5. Useful to see if model is confident or not

### Table: Team Rankings by KPIs

1. Create new Visualization → **Table**
2. Select `nba_team_metrics` index
3. **Metrics** (bottom):
   - Avg of `win_rate_last5` (label: "Win Rate")
   - Avg of `avg_points_last5` (label: "Avg Points")
   - Avg of `home_away_diff` (label: "Home Advantage")
   - Avg of `rest_days` (label: "Avg Rest Days")
4. **Buckets** (top):
   - Split rows: Terms on `team_name` (size: 30, order by: Win Rate desc)
   - Split table: Terms on `conference` (optional)
5. Sort by: Win Rate descending
6. Enable "Show partial rows" if some teams have missing data

### Bar Chart: Schedule Difficulty by Team

1. Visualization → Bar Chart
2. Index: nba_team_metrics
3. Y-Axis: Avg of schedule_difficulty_next5
4. X-Axis: Terms on team_name
5. Sort: Descending

### Table: SOS Ranking

1. Visualization → Table
2. Index: nba_team_metrics
3. Metrics: 
   - Avg schedule_difficulty_next5
   - Avg home_games_next5
   - Avg away_games_next5
4. Buckets: team_name (size: 30, sort by schedule_difficulty_next5: Descending)
