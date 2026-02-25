# Strength of Schedule (SOS) KPI - Design

## Overview

Add a new KPI to measure the difficulty of each team's upcoming schedule based on opponent strength, home/away factor, and back-to-back games.

## Data Flow

```
games_df → compute_schedule_difficulty → team_metrics (with SOS)
                                         → Kibana dashboard
```

## KPI Definition

### Strength of Schedule (SOS)

| Metric | Description |
|--------|-------------|
| `schedule_difficulty_next5` | Avg difficulty of next 5 games |
| `schedule_difficulty_next10` | Avg difficulty of next 10 games |
| `home_games_next5` | Number of home games in next 5 |
| `away_games_next5` | Number of away games in next 5 |
| `b2b_games_next5` | Number of back-to-back games in next 5 |
| `sos_rank` | Rank (1 = hardest schedule) |

### Difficulty Formula

```
difficulty = opponent_win_rate 
  × (1 + (is_away? 0.20 : 0))      
  × (1 + (is_b2b? 0.20 : 0))  
```

**Factors:**
- **Opponent Form** (50%): Win rate last5 of opponent
- **Home/Away** (25%): +20% if away game
- **Travel/Back-to-back** (25%): +20% if B2B

## Implementation

### 1. Update `combine_metrics.py`

Add `compute_schedule_difficulty()` function:

```python
def compute_schedule_difficulty(team_games_df, horizon=5):
    # Get all future games for each team
    # Join with opponent's win_rate_last5
    # Calculate difficulty per game
    # Aggregate by team (avg for next N games)
```

### 2. Update Output Schema

Add to `team_metrics`:
- `schedule_difficulty_next5` (float)
- `schedule_difficulty_next10` (float)
- `home_games_next5` (int)
- `away_games_next5` (int)
- `b2b_games_next5` (int)
- `sos_rank` (int)

### 3. Kibana Visualizations

New visualizations:
1. **Schedule Difficulty by Team** - Bar chart
2. **SOS Ranking Table** - Data table with all teams
3. **Home/Away Distribution Next 5** - Pie chart

## Refresh

- **Frequency**: Daily (after each game day completes)
- **Data**: Uses current `win_rate_last5` of opponents (dynamic)

## Example Output

| Team | SOS (Next 5) | Rank | Home | Away | B2B |
|------|-------------|------|------|------|-----|
| Lakers | 0.68 | 5 | 2 | 3 | 1 |
| Celtics | 0.72 | 2 | 3 | 2 | 0 |
| Warriors | 0.75 | 1 | 1 | 4 | 2 |

## Trade-offs

- **Pro**: More accurate than simple opponent strength (adds context)
- **Pro**: Dynamic (updates daily with team form changes)
- **Con**: Requires look-ahead logic in Spark (window functions)
- **Con**: First few games of season have limited data

## Alternative Approaches Considered

1. **Static SOS**: Use season-average opponent strength → Rejected (too static)
2. **Pythagorean Expectation**: Use point differential instead of win rate → Deferred (v2)
3. **Elo-based**: Use external Elo ratings → Deferred (requires external data)
