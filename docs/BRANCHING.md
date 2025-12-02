Backup before Supabase switch
git checkout main
git pull
git checkout -b backup/pre-supabase-streamlit-YYYYMMDD
git push -u origin backup/pre-supabase-streamlit-YYYYMMDD
git checkout main

Backup before GWR integration
git checkout main
git pull
git checkout -b backup/pre-gwr-integration-YYYYMMDD
git push -u origin backup/pre-gwr-integration-YYYYMMDD
git checkout main
