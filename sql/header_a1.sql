-- !preview conn=con

-- madc_integ.
--   public.
--     header_a1.
--       dob
--       sex, fu_sex
--       race
--       educ

SELECT 
  ptid, form_date, 
  dob, 
  COALESCE(sex, fu_sex) AS sex, 
  race, 
  educ
FROM public.header_a1
WHERE
  ptid      >= 'UM00000543'::text AND
  form_date >= '2017-03-15'::date
ORDER BY ptid, form_date;
