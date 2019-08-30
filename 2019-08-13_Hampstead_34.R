#!/usr/bin/env Rscript

# 2019-08-13_Hampstead_34.R

# Identify 12 cognitively intact and 12 with dementia due to Alzheimer's disease
# [madc_integ, d1.normcog + d1.demented + d1.alzdis + d1.alzdisif // fu_] who:
#   a. All donated blood [MiNDSet blood_draw, blood_drawn + blood_draw_date]
#   b. Are Matched on age, sex, race, and education 
#      [madc_integ, header_a1.dob + header_a1.sex + header_a1.race + 
#      header_a1.educ // fu_]
#   c. Are As free of vascular disease as possible (ideally none would have 
#      hypertension, hyperlipidemia, or diabetes; obviously no prior strokes 
#      or TIAs). [madc_integ, a5.hyperten + a5.hypercho + a5.diabet + 
#      a5.cbstroke + a5.cbtia // fu_]


# **************
# Libraries ----

# Database
library(DBI)
# Munging
library(dplyr)
library(readr)
library(stringr)
library(FuzzyDateJoin)


# *********************
# Config / Helpers ----

source("~/Box Sync/Documents/R_helpers/config.R")
source("~/Box Sync/Documents/R_helpers/helpers.R")


# ******************************
# Get MiNDSET Registry Data ----

# Define MiNDSet fields
fields_ms_raw <-
  c(
    "subject_id"
    , "exam_date"
    , "sex_value"
    , "race_value"
    , "ed_level"
  )
fields_msbld_raw <-
  c(
    "subject_id"
    , "blood_draw_date"
    , "blood_drawn"
  )

fields_ms    <- fields_ms_raw %>% paste(collapse = ",")
fields_msbld <- fields_msbld_raw %>% paste(collapse = ",")

# Retrieve JSON object via REDCap API
json_ms <-
  export_redcap_records(
    uri    = REDCAP_API_URI,
    token  = REDCAP_API_TOKEN_MINDSET,
    fields = fields_ms,
    filterLogic = paste0("(",
                         "[subject_id] >= 'UM00000543'",
                         " AND ",
                         "[subject_id] < 'UM00002000'",
                         " AND ",
                         "[exam_date] >= '2017-03-15'",
                         ")")
  )

json_msbld <-
  export_redcap_records(
    uri    = REDCAP_API_URI,
    token  = REDCAP_API_TOKEN_MINDSET,
    fields = fields_msbld,
    filterLogic = paste0("(",
                         "[subject_id] >= 'UM00000543'",
                         " AND ",
                         "[subject_id] < 'UM00002000'",
                         " AND ",
                         "[blood_draw_date] != ''",
                         " AND ",
                         "[blood_drawn] = 1",
                         ")")
  )

# Build df/tibble from JSON object
df_ms <- 
  json_ms %>% 
  jsonlite::fromJSON() %>% 
  select(-redcap_event_name) %>% 
  as_tibble %>% 
  na_if("") %>% 
  mutate_at(vars(subject_id), as.character) %>% 
  mutate_at(vars(race_value, sex_value, ed_level), as.integer) %>% 
  mutate_at(vars(exam_date), as.Date)

df_msbld <-
  json_msbld %>% 
  jsonlite::fromJSON() %>% 
  select(-redcap_event_name) %>% 
  as_tibble %>% 
  na_if("") %>% 
  mutate_at(vars(subject_id), as.character) %>% 
  mutate_at(vars(blood_drawn), as.integer) %>% 
  mutate_at(vars(blood_draw_date), as.Date)

df_ms_msbld <-
  outer_left(df_ms, df_msbld,
             x_id_col = "subject_id", y_id_col = "subject_id",
             x_date_col = "exam_date", y_date_col = "blood_draw_date",
             x_intvl_less = 90L, x_intvl_more = 90L) %>% 
  select(subject_id = subject_id_x
         , exam_date
         , sex_value
         , race_value
         , ed_level
         , blood_draw_date
         , blood_drawn
         , everything()
         , -subject_id_y)

df_ms_msbld_flt <-
  df_ms_msbld %>% 
  filter(blood_drawn == 1L)


# ******************
# Connect to db ----

con <- 
  dbConnect(RPostgres::Postgres(),
            service  = "madcbrain pgsql madc_integ", # ~/.pg_service.conf
            user     = rstudioapi::askForPassword("PostgreSQL Username:"),
            password = rstudioapi::askForPassword("PostgreSQL Password:"))
dbListTables(con)


# _ Get db data ----

# Get `header_a1` data
res_hd_a1 <- dbSendQuery(con, read_file("sql/header_a1.sql"))
df_hd_a1  <- dbFetch(res_hd_a1)
dbClearResult(res_hd_a1); rm(res_hd_a1)

# Get `a5` data
res_a5 <- dbSendQuery(con, read_file("sql/a5.sql"))
df_a5  <- dbFetch(res_a5)
dbClearResult(res_a5); rm(res_a5)

# Get `d1` data
res_d1 <- dbSendQuery(con, read_file("sql/d1.sql"))
df_d1  <- dbFetch(res_d1)
dbClearResult(res_d1); rm(res_d1)

dbDisconnect(con); rm(con)


# _ Clean db data ----

# Clean `header_a1` data
df_hd_a1_flt <-
  df_hd_a1 %>% 
  propagate_value(ptid, form_date, dob) %>%
  propagate_value(ptid, form_date, sex) %>%
  propagate_value(ptid, form_date, race) %>%
  propagate_value(ptid, form_date, educ) %>%
  # Get latest visit
  get_visit_n(ptid, form_date, Inf)

# Clean `a5` data
df_a5_flt <-
  df_a5 %>% 
  # keep only records with comorbidity field values
  get_nonempty_records(
    relevant_fields = 
      c(
        "hyperten"
        , "hypercho"
        , "diabetes"
        , "cbstroke"
        , "cbtia"
      )
  ) %>% 
  # Get latest visit
  get_visit_n(ptid, form_date, Inf) %>% 
  select(-form_date)

# Clean `d1` data
df_d1_flt <-
  df_d1 %>% 
  # Get latest visit
  get_visit_n(ptid, form_date, Inf) %>% 
  select(-form_date)


# _ Mutate db data ----

# Calculate age from `dob` and `form_date`
df_hd_a1_flt_mut <-
  df_hd_a1_flt %>% 
  calculate_age(dob, form_date) %>% 
  select(-dob, -form_date, -age_years, -age_units)


# _ Join db data ----

df_hda1_a5_d1 <-
  left_join(df_hd_a1_flt_mut, df_a5_flt, by = "ptid") %>% 
  left_join(., df_d1_flt, by = "ptid") %>% 
  mutate_at(vars(ptid), as.character) %>% 
  mutate_at(vars(age_exact), as.numeric) %>% 
  mutate_at(
    vars(
      sex
      , race
      , educ
      , hyperten
      , hypercho
      , diabetes
      , cbstroke
      , cbtia
      , normcog
      , demented
      , alzdis
      , alzdisif
    ),
    as.integer
  )


# _ Filter joined data ----

# Get unique IDs from `df_ms_bld`
ids_ms <- 
  df_ms_msbld_flt %>% 
  distinct(subject_id) %>% 
  pull %>% 
  sort

df_hda1_a5_d1_bld <-
  df_hda1_a5_d1 %>% 
  filter(ptid %in% ids_ms)

df_hda1_a5_d1_bld_nrmalz <-
  df_hda1_a5_d1_bld %>% 
  filter(
    normcog == 1L | 
      (demented == 1L & alzdis == 1L & alzdisif == 1L)
  ) 

df_hda1_a5_d1_bld_nrmalz_crdvsc <-
  df_hda1_a5_d1_bld_nrmalz %>% 
  filter(
    hyperten != 1L &
    hypercho != 1L & 
      diabetes != 1L & 
      cbstroke != 1L &
      cbtia    != 1L
  )

df__normcog <- 
  df_hda1_a5_d1_bld_nrmalz_crdvsc %>% 
  filter(normcog == 1L)

df__dementd <-
  df_hda1_a5_d1_bld_nrmalz_crdvsc %>% 
  filter(demented == 1L & alzdis == 1L & alzdisif == 1L)


# *******************************
# Calculate cosine distances ----

library(ggplot2)

df_hda1_a5_d1_bld_nrmalz_crdvsc %>% 
  ggplot(aes(x = age_exact)) +
  geom_histogram(binwidth = 5)

df_hda1_a5_d1_bld_nrmalz_crdvsc %>% 
  ggplot(aes(x = educ)) +
  geom_histogram(binwidth = 1)

age_exact_med <- median(df_hda1_a5_d1_bld_nrmalz_crdvsc$age_exact, na.rm = TRUE)
educ_med <- median(df_hda1_a5_d1_bld_nrmalz_crdvsc$educ, na.rm = TRUE)

scl_fctr <- 2
df_cos <-
  df_hda1_a5_d1_bld_nrmalz_crdvsc %>% 
  mutate(male_01   = if_else(sex == 1L, 1L, 0L) * scl_fctr,
         female_01 = if_else(sex == 2L, 1L, 0L) * scl_fctr,
         white_01  = if_else(race == 1L, 1L, 0L) * scl_fctr,
         black_01  = if_else(race == 2L, 1L, 0L) * scl_fctr,
         age_MAD   = age_exact - age_exact_med,
         educ_MAD  = educ - educ_med) %>% 
  mutate(age_MAD_01 = 
           (age_MAD - min(.$age_MAD)) / 
           (max(.$age_MAD) - min(.$age_MAD)),
         educ_MAD_01 = 
           (educ_MAD - min(.$educ_MAD)) / 
           (max(.$educ_MAD) - min(.$educ_MAD))) # %>% 
# select(ptid,
#        normcog,
#        male_01, female_01,
#        white_01, black_01,
#        age_MAD_rs,
#        educ_MAD_rs)

df_cos_nrm <- df_cos %>% 
  filter(normcog == 1L)
df_cos_dem <- df_cos %>% 
  filter(normcog != 1L)

dot_product <- function(x, y) sum(x * y)
magnitude <- function(x) sqrt(sum(x^2))
cos_similarity <- function(x, y) {
  dot_product(x, y) / 
    ( magnitude(x) * magnitude(y) )
}

MEAS_COLS <- 
  c(
    "male_01"
    , "female_01"
    , "white_01"
    , "black_01"
    , "age_MAD_01"
    , "educ_MAD_01"
  )

# mtrx_nrm_cos_sim <-
#   matrix(NA_real_,
#          nrow = nrow(df_cos_nrm),
#          ncol = nrow(df_cos_dem),
#          dimnames = list(df_cos_nrm$ptid, df_cos_dem$ptid))
mtrx_dem_cos_sim <- 
  matrix(NA_real_,
         nrow = nrow(df_cos_dem),
         ncol = nrow(df_cos_nrm),
         dimnames = list(df_cos_dem$ptid, df_cos_nrm$ptid))

# # Populate `mtrx_nrm_cos_sim`
# for (ptid_nrm in rownames(mtrx_nrm_cos_sim)) {
#   for (ptid_dem in colnames(mtrx_nrm_cos_sim)) {
#     mtrx_nrm_cos_sim[ptid_nrm, ptid_dem] <-
#       cos_similarity(
#         as.numeric(df_cos_nrm[which(df_cos_nrm$ptid == ptid_nrm), MEAS_COLS]),
#         as.numeric(df_cos_dem[which(df_cos_dem$ptid == ptid_dem), MEAS_COLS])
#         )
#   }
# }

# Populate `mtrx_dem_cos_sim`
for (ptid_dem in rownames(mtrx_dem_cos_sim)) {
  for (ptid_nrm in colnames(mtrx_dem_cos_sim)) {
    mtrx_dem_cos_sim[ptid_dem, ptid_nrm] <-
      cos_similarity(
        as.numeric(df_cos_dem[which(df_cos_dem$ptid == ptid_dem), MEAS_COLS]),
        as.numeric(df_cos_nrm[which(df_cos_nrm$ptid == ptid_nrm), MEAS_COLS])
      )
  }
}


# df_a <- tibble(id = "UM01", col_1 = "foo", col_2 = "bar", col_3 = "baz")
# mtrx_a <- matrix(c(0.0, 0.1, 0.2, 0.3), nrow = 1, ncol = 4)
# # df_b <- tibble(id = "UM01") %>% 
# #   bind_cols(df_a[1, 2:ncol(df_a)])
# df_b <- tibble(id = "UM01") %>% 
#   bind_cols(as_tibble(t(as.matrix(mtrx_a[1, 2:(ncol(mtrx_a))])), 
#                       .name_repair = ~ paste0("col_", 1:3)))


dfs_ptid_dem <- list()

for (ptid_dem in rownames(mtrx_dem_cos_sim)) {
  # print(
  #   paste(
  #     ptid_dem,
  #     which(mtrx_dem_cos_sim[ptid_dem, ] == max(mtrx_dem_cos_sim[ptid_dem, ])),
  #     max(mtrx_dem_cos_sim[ptid_dem, ])
  #   )
  # )
  dfs_ptid_dem[[ptid_dem]] <-
    tibble(id = ptid_dem) %>%
    bind_cols(
      as_tibble(
        t(as.matrix(
          sort(mtrx_dem_cos_sim[ptid_dem, 1:(ncol(mtrx_dem_cos_sim))], 
               decreasing = TRUE))),
        .name_repair = 
          ~ names(sort(mtrx_dem_cos_sim[ptid_dem, 1:(ncol(mtrx_dem_cos_sim))], 
                       decreasing = TRUE))
      ))
}

for(ptid_dem in names(dfs_ptid_dem)) {
  print(dfs_ptid_dem[ptid_dem][[ptid_dem]][, 1:8])
}

write_csv(df__normcog, "df__normcog.csv", na = "")
write_csv(df__dementd, "df__dementd.csv", na = "")


# # **************
# # Save Data ----
#
# save.image(
#   paste0("RData/2019-08-13_Hampstead_34_", Sys.Date(), ".RData"))


###@    #==--  :  --==#    @##==---==##@##==---==##@    #==--  :  --==#    @###
#==##@    #==-- --==#    @##==---==##@   @##==---==##@    #==-- --==#    @##==#
#--==##@    #==-==#    @##==---==##@   #   @##==---==##@    #==-==#    @##==--#
#=---==##@    #=#    @##==---==##@    #=#    @##==---==##@    #=#    @##==---=#
##==---==##@   #   @##==---==##@    #==-==#    @##==---==##@   #   @##==---==##
#@##==---==##@   @##==---==##@    #==-- --==#    @##==---==##@   @##==---==##@#
#  @##==---==##@##==---==##@    EXTRA  :  SPACE    @##==---==##@##==---==##@  #
#@##==---==##@   @##==---==##@    #==-- --==#    @##==---==##@   @##==---==##@#
##==---==##@   #   @##==---==##@    #==-==#    @##==---==##@   #   @##==---==##
#=---==##@    #=#    @##==---==##@    #=#    @##==---==##@    #=#    @##==---=#
#--==##@    #==-==#    @##==---==##@   #   @##==---==##@    #==-==#    @##==--#
#==##@    #==-- --==#    @##==---==##@   @##==---==##@    #==-- --==#    @##==#
###@    #==--  :  --==#    @##==---==##@##==---==##@    #==--  :  --==#    @###
#==##@    #==-- --==#    @##==---==##@   @##==---==##@    #==-- --==#    @##==#
#--==##@    #==-==#    @##==---==##@   #   @##==---==##@    #==-==#    @##==--#
#=---==##@    #=#    @##==---==##@    #=#    @##==---==##@    #=#    @##==---=#
##==---==##@   #   @##==---==##@    #==-==#    @##==---==##@   #   @##==---==##
#@##==---==##@   @##==---==##@    #==-- --==#    @##==---==##@   @##==---==##@#
#  @##==---==##@##==---==##@    EXTRA  :  SPACE    @##==---==##@##==---==##@  #
#@##==---==##@   @##==---==##@    #==-- --==#    @##==---==##@   @##==---==##@#
##==---==##@   #   @##==---==##@    #==-==#    @##==---==##@   #   @##==---==##
#=---==##@    #=#    @##==---==##@    #=#    @##==---==##@    #=#    @##==---=#
#--==##@    #==-==#    @##==---==##@   #   @##==---==##@    #==-==#    @##==--#
#==##@    #==-- --==#    @##==---==##@   @##==---==##@    #==-- --==#    @##==#
###@    #==--  :  --==#    @##==---==##@##==---==##@    #==--  :  --==#    @###
