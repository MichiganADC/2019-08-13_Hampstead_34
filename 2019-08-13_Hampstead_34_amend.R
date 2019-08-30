# 2019-08-13_Hampstead_34_amend.R

# This is an amendment to the original data request.


# Globals & Helpers
source("~/Box Sync/Documents/R_helpers/config.R")
source("~/Box Sync/Documents/R_helpers/helpers.R")


# Libraries 
library(dplyr)
library(readr)
library(readxl)
library(tidyr)


# Load "match notes" worksheet

df_match_notes <- 
  read_excel("match_notes.xlsx",
             sheet = "Sheet1",
             col_names = TRUE,
             col_types = "guess")

df_match_notes_dem <-
  df_match_notes %>% 
  select(
    ptid   = ptid_dem
    , sex  = sex_dem
    , race = race_dem
    , educ = educ_dem
    , age  = age_dem
  ) %>% 
  mutate(dx = "Dementia dt AD")

df_match_notes_nl <-
  df_match_notes %>% 
  select(
    ptid   = ptid_nl
    , sex  = sex_nl
    , race = race_nl
    , educ = educ_nl
    , age  = age_nl
  ) %>% 
  mutate(dx = "Normal")

df_match_long <-
  bind_rows(df_match_notes_dem , df_match_notes_nl) %>% 
  mutate(age = round(age, 1))


# Get Neuropysch Data from `madc_integ`

con <- 
  dbConnect(RPostgres::Postgres(),
            service  = "madcbrain pgsql madc_integ", # ~/.pg_service.conf
            user     = rstudioapi::askForPassword("PostgreSQL Username:"),
            password = rstudioapi::askForPassword("PostgreSQL Password:"))
dbListTables(con)
  
# c1c2, jolo, hvlt, emory_wcst, wtar, cowa_cfl, oltt

# c1c2
res_c1c2 <- dbSendQuery(con, read_file("sql_amend/c1c2.sql"))
df_c1c2  <- dbFetch(res_c1c2)
dbClearResult(res_c1c2); rm(res_c1c2)

# jolo
res_jolo <- dbSendQuery(con, read_file("sql_amend/jolo.sql"))
df_jolo  <- dbFetch(res_jolo)
dbClearResult(res_jolo); rm(res_jolo)

# hvlt
res_hvlt <- dbSendQuery(con, read_file("sql_amend/hvlt.sql"))
df_hvlt  <- dbFetch(res_hvlt)
dbClearResult(res_hvlt); rm(res_hvlt)

# emory_wcst
res_emory_wcst <- dbSendQuery(con, read_file("sql_amend/emory_wcst.sql"))
df_emory_wcst  <- dbFetch(res_emory_wcst)
dbClearResult(res_emory_wcst); rm(res_emory_wcst)

# wtar
res_wtar <- dbSendQuery(con, read_file("sql_amend/wtar.sql"))
df_wtar  <- dbFetch(res_wtar)
dbClearResult(res_wtar); rm(res_wtar)

# cowa_cfl
res_cowa_cfl <- dbSendQuery(con, read_file("sql_amend/cowa_cfl.sql"))
df_cowa_cfl  <- dbFetch(res_cowa_cfl)
dbClearResult(res_cowa_cfl); rm(res_cowa_cfl)

# oltt
res_oltt <- dbSendQuery(con, read_file("sql_amend/oltt.sql"))
df_oltt  <- dbFetch(res_oltt)
dbClearResult(res_oltt); rm(res_oltt)


# Join data

df_neuropsych_last_visit <-
  df_c1c2 %>% 
  left_join(df_jolo,       by = c("ptid", "form_date")) %>% 
  left_join(df_hvlt,       by = c("ptid", "form_date")) %>% 
  left_join(df_emory_wcst, by = c("ptid", "form_date")) %>% 
  left_join(df_wtar,       by = c("ptid", "form_date")) %>% 
  left_join(df_cowa_cfl,   by = c("ptid", "form_date")) %>% 
  left_join(df_oltt,       by = c("ptid", "form_date")) %>% 
  coalesce_ift_cols() %>% 
  get_visit_n(ptid, form_date, Inf)

df_match_neuropsych <-
  df_match_long %>% 
  left_join(df_neuropsych_last_visit, by = "ptid")


# Write to CSV

write_csv(df_match_neuropsych, "2019-08-13_Hampstead_34_match_NP.csv", na = "")






















