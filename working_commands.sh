python src/parsely_analysis/monthly_auth_rank.py \
        inputs/posts-export-by-page-views-Jun-01-2024-Jul-30-2025-indyweek-com.csv \
        --format compact \
        --top-n 5 \
        --ignore-authors "INDY staff" \
        --ignore-authors "Staff" \
        --ignore-authors "adminnewspack" \
        --ignore-authors "INDY Sales" \
        --after-date 2024-07-01

python src/parsely_analysis/monthly_auth_rank.py \
        inputs/posts-export-by-page-views-Jun-01-2024-Jul-30-2025-indyweek-com.csv \
        --top-n 5 \
        --ignore-authors "INDY staff" \
        --ignore-authors "Staff" \
        --ignore-authors "adminnewspack" \
        --ignore-authors "INDY Sales" \
        --after-date 2024-07-01



python src/parsely_analysis/journalist_metrics.py \
    inputs/posts-export-by-page-views-Jun-01-2024-Jul-30-2025-indyweek-com.csv \
    --top-n 5 \
    --after-date 2024-07-01    


----

python src/parsely_analysis/monthly_auth_rank.py \
        inputs/posts-export-by-page-views-Jun-01-2024-Jul-30-2025-indyweek-com.csv \
        --format compact \
        --top-n 3 \
        --ignore-authors "INDY staff" \
        --ignore-authors "Staff" \
        --ignore-authors "adminnewspack" \
        --ignore-authors "INDY Sales" \
        --after-date 2024-07-01

python src/parsely_analysis/monthly_auth_rank.py \
        inputs/posts-export-by-page-views-Jun-01-2024-Jul-30-2025-indyweek-com.csv \
        --top-n 3 \
        --ignore-authors "INDY staff" \
        --ignore-authors "Staff" \
        --ignore-authors "adminnewspack" \
        --ignore-authors "INDY Sales" \
        --after-date 2024-07-01



python src/parsely_analysis/journalist_metrics.py \
    inputs/posts-export-by-page-views-Jun-01-2024-Jul-30-2025-indyweek-com.csv \
    --top-n 7 \
    --after-date 2024-07-01    



##############################
##############################
##############################
##############################
##############################
##############################

# new script

inputs/posts-export-by-page-views-Jun-01-2024-Jul-30-2025-indyweek-com.csv



python src/analyze_journalists.py \
    --input-csv inputs/posts-export-by-page-views-Jun-01-2024-Jul-30-2025-indyweek-com.csv \
    --top-n 5 \
    --ignore-authors "INDY staff" \
    --ignore-authors "Staff" \
    --ignore-authors "adminnewspack" \
    --start-date 2024-07-01 



python src/analyze_journalists.py \
    --input-csv inputs/posts-export-by-page-views-Jun-01-2024-Jul-30-2025-indyweek-com.csv \
    --top-n 5 \
    --ignore-authors "INDY staff" \
    --ignore-authors "Staff" \
    --ignore-authors "adminnewspack" \
    --start-date 2024-06-01 



python src/analyze_journalists.py \
    --input-csv inputs/posts-export-by-page-views-Jun-01-2024-Jul-30-2025-indyweek-com.csv \
    --top-n 5 \
    --ignore-authors "INDY staff" \
    --ignore-authors "Staff" \
    --ignore-authors "adminnewspack" \
    --start-date 2023-09-01 



python src/analyze_journalists.py \
    --input-csv inputs/posts-export-by-page-views-Jun-01-2024-Jul-30-2025-indyweek-com.csv \
    --top-n 5 \
    --ignore-authors "INDY staff" \
    --ignore-authors "Staff" \
    --ignore-authors "adminnewspack" \
    --start-date 2018-01-01 




##############################
##############################
##############################
##############################
##############################
##############################




python src/analyze_7_30_10pm_6.py \
    inputs/posts-export-by-page-views-Jun-01-2024-Jul-30-2025-indyweek-com.csv \
    --top-n-aggregate 10 \
    --top-n-monthly 3 \
    --ignore-authors "INDY staff" \
    --ignore-authors "INDY Sales" \
    --ignore-authors "Staff" \
    --ignore-authors "adminnewspack" \
    --after-date 2024-07-01 


inputs/posts-export-by-page-views-Jul-01-2024-Jul-29-2025-indyweek-com_7_29_1am.csv
inputs/posts-export-by-page-views-Jul-01-2024-Jul-29-2025-indyweek-com_7_29_10am.csv
inputs/posts-export-by-page-views-Jun-01-2024-Jul-30-2025-indyweek-com_7_30_4_30pm.csv
inputs/posts-export-by-page-views-Jun-01-2024-Jul-30-2025-indyweek-com_7_30_10pm.csv
inputs/posts-export-by-returning-visitors-Jul-01-2024-Jul-28-2025-indyweek-com_7_28_11am.csv


python src/analyze_7_30_10pm_6.py \
    inputs/posts-export-by-page-views-Jul-01-2024-Jul-29-2025-indyweek-com_7_29_1am.csv \
    --top-n-aggregate 3 \
    --top-n-monthly 3 \
    --ignore-authors "INDY staff" \
    --ignore-authors "INDY Sales" \
    --ignore-authors "Staff" \
    --ignore-authors "adminnewspack" \
    --after-date 2024-07-01 


python src/analyze_7_30_10pm_6.py \
    inputs/posts-export-by-page-views-Jul-01-2024-Jul-29-2025-indyweek-com_7_29_10am.csv \
    --top-n-aggregate 3 \
    --top-n-monthly 3 \
    --ignore-authors "INDY staff" \
    --ignore-authors "INDY Sales" \
    --ignore-authors "Staff" \
    --ignore-authors "adminnewspack" \
    --after-date 2024-07-01 


python src/analyze_7_30_10pm_6.py \
    inputs/posts-export-by-page-views-Jun-01-2024-Jul-30-2025-indyweek-com_7_30_4_30pm.csv \
    --top-n-aggregate 3 \
    --top-n-monthly 3 \
    --ignore-authors "INDY staff" \
    --ignore-authors "INDY Sales" \
    --ignore-authors "Staff" \
    --ignore-authors "adminnewspack" \
    --after-date 2024-07-01 


python src/analyze_7_30_10pm_6.py \
    keep_inputs/posts-export-by-page-views-Jun-01-2024-Jul-30-2025-indyweek-com_7_30_11pm.csv \
    --top-n-aggregate 3 \
    --top-n-monthly 3 \
    --ignore-authors "INDY staff" \
    --ignore-authors "INDY Sales" \
    --ignore-authors "Staff" \
    --ignore-authors "adminnewspack" \
    --after-date 2024-08-01 


python src/analyze_7_30_10pm_6.py \
    inputs/posts-export-by-returning-visitors-Jul-01-2024-Jul-28-2025-indyweek-com_7_28_11am.csv \
    --top-n-aggregate 6 \
    --top-n-monthly 3 \
    --ignore-authors "INDY staff" \
    --ignore-authors "INDY Sales" \
    --ignore-authors "Staff" \
    --ignore-authors "adminnewspack" \
    --after-date 2024-08-01 

    https://claude.ai/chat/57f00804-7859-4db8-8c62-dde41a50ea62


############################################
############################################
############################################
############################################

python src/splitter_1.py \
keep_inputs/posts-export-by-page-views-Jun-01-2024-Jul-30-2025-indyweek-com_7_30_11pm.csv \
2024-06-01 -o splitter_output_directory


#########
#########
#########
#########
#########
#########
#########




python src/script.py \
    --monthly-datadir input_v731/monthly \
    --range-datadir input_v731/range \
    --top-n-range 6 \
    --top-n-monthly 3 \
    --ignore-authors "INDY staff" \
    --ignore-authors "INDY Sales" \
    --ignore-authors "Staff" \
    --ignore-authors "adminnewspack"

    python src/v731_2.py \
    --monthly-datadir input_v731/monthly \
    --range-datadir input_v731/range \
    --top-n-range 6 \
    --top-n-monthly 3 \
    --ignore-authors "INDY staff" \
    --ignore-authors "INDY Sales" \
    --ignore-authors "Staff" \
    --ignore-authors "adminnewspack"

    ###########


python src/v731_20.py \
    --monthly-datadir input_v731/monthly \
    --range-datadir input_v731/range \
    --top-n-range 6 \
    --top-n-monthly 3 \
    --ignore-authors "INDY staff" \
    --ignore-authors "INDY Sales" \
    --ignore-authors "Staff" \
    --ignore-authors "adminnewspack"