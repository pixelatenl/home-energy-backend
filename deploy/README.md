# Database (TimescaleDB) for the home energy management system

This folder contains the project's data layer: the TimescaleDB container and the nightly backup, fully self-contained in `deploy/`.

## Files
   File | Contents |
 |---|---|
 | docker-compose.yml | TimescaleDB container plus nightly backup container |
 | deploy-db.sh | Deployment and verification script for the server |
 | initdb/init.sql | Data model: hypertables, continuous aggregates, retention |
 | migrations/ | Schema changes after the initial deploy, applied manually |
 | .env.example | Example of the environment variables |

## Installation on the server

```bash
git pull
cd deploy
./deploy-db.sh