#!/bin/bash

set -e

ELASTIC_HOST="${ELASTIC_HOST:-http://localhost:9200}"
KIBANA_HOST="${KIBANA_HOST:-http://localhost:5601}"

echo "Waiting for Elasticsearch..."
until curl -s "$ELASTIC_HOST/_cluster/health" | grep -q '"status":"green"\|"status":"yellow"'; do
    sleep 2
done
echo "Elasticsearch is ready!"

echo "Waiting for Kibana..."
until curl -s "$KIBANA_HOST/api/status" | grep -q '"overall":{"state":"healthy"'; do
    sleep 2
done
echo "Kibana is ready!"

echo "Creating index patterns..."

curl -X POST "$KIBANA_HOST/api/saved_objects/index-pattern/nba_team_metrics" \
    -H "kbn-xsrf: true" \
    -H "Content-Type: application/json" \
    -d '{
        "attributes": {
            "title": "nba_team_metrics",
            "timeFieldName": "game_date"
        }
    }' 2>/dev/null || true

curl -X POST "$KIBANA_HOST/api/saved_objects/index-pattern/nba_match_metrics" \
    -H "kbn-xsrf: true" \
    -H "Content-Type: application/json" \
    -d '{
        "attributes": {
            "title": "nba_match_metrics",
            "timeFieldName": "game_date"
        }
    }' 2>/dev/null || true

echo "Index patterns created!"

echo "Setting default index pattern..."
curl -X POST "$KIBANA_HOST/api/kibana/settings/defaultIndex" \
    -H "kbn-xsrf: true" \
    -H "Content-Type: application/json" \
    -d '{"value":"nba_team_metrics"}' 2>/dev/null || true

echo ""
echo "=============================================="
echo "Kibana setup complete!"
echo "=============================================="
echo ""
echo "Go to http://localhost:5601"
echo ""
echo "1. Go to 'Stack Management' > 'Index Patterns'"
echo "   Verify: nba_team_metrics and nba_match_metrics exist"
echo ""
echo "2. Go to 'Analytics' > 'Dashboard'"
echo "   Create your visualizations"
echo ""
echo "3. Key fields to visualize:"
echo "   nba_team_metrics:"
echo "     - team_name, conference, division"
echo "     - win_rate_last5, avg_points_last5"
echo "     - home_away_diff, is_home"
echo "     - game_date, season"
echo ""
echo "   nba_match_metrics:"
echo "     - home_team_id, visitor_team_id"
echo "     - home_win, home_team_score, visitor_team_score"
echo "     - win_probability_home"
echo "     - game_date, season"
echo ""
echo "=============================================="
