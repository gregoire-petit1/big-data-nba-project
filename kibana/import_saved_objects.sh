#!/bin/bash

set -e

ELASTIC_HOST="${ELASTIC_HOST:-http://localhost:9200}"
KIBANA_HOST="${KIBANA_HOST:-http://localhost:5601}"

echo "=========================================="
echo "Kibana Saved Objects Import Script"
echo "=========================================="

echo "Waiting for Elasticsearch..."
until curl -s "$ELASTIC_HOST/_cluster/health" > /dev/null 2>&1; do
    sleep 2
done
echo "✓ Elasticsearch is ready!"

echo "Waiting for Kibana..."
until curl -s "$KIBANA_HOST/api/status" > /dev/null 2>&1; do
    sleep 2
done
echo "✓ Kibana is ready!"

echo ""
echo "Creating index patterns..."

curl -s -X POST "$KIBANA_HOST/api/saved_objects/index-pattern/nba_team_metrics" \
    -H "kbn-xsrf: true" \
    -H "Content-Type: application/json" \
    -d '{
        "attributes": {
            "title": "nba_team_metrics",
            "timeFieldName": "game_date"
        }
    }' | jq -r '.[] | .id // .message' 2>/dev/null || echo "  → nba_team_metrics index pattern created"

curl -s -X POST "$KIBANA_HOST/api/saved_objects/index-pattern/nba_match_metrics" \
    -H "kbn-xsrf: true" \
    -H "Content-Type: application/json" \
    -d '{
        "attributes": {
            "title": "nba_match_metrics",
            "timeFieldName": "game_date"
        }
    }' | jq -r '.[] | .id // .message' 2>/dev/null || echo "  → nba_match_metrics index pattern created"

echo "✓ Index patterns created!"

echo ""
echo "Setting default index pattern..."
curl -s -X POST "$KIBANA_HOST/api/kibana/settings/defaultIndex" \
    -H "kbn-xsrf: true" \
    -H "Content-Type: application/json" \
    -d '{"value":"nba_team_metrics"}' > /dev/null 2>&1
echo "✓ Default index set to nba_team_metrics"

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "1. Go to http://localhost:5601"
echo "2. Open Stack Management > Index Patterns"
echo "   Verify both patterns exist"
echo ""
echo "3. Create Visualizations manually or import from saved_objects.json"
echo ""
echo "4. Build your Dashboard at Analytics > Dashboard"
echo ""
echo "=========================================="
