-- !preview conn=con

-- madc_integ
--   public.
--     a5.
--       hyperten, fu_hyperten
--       hypercho, fu_hypercho
--       diabetes, fu_diabetes
--       cbstroke, fu_cbstroke
--       cbtia, fu_cbtia
--       

SELECT 
  ptid, form_date, 
  -- hyperten, fu_hyperten,
  -- hypercho, fu_hypercho,
  -- diabetes, fu_diabetes,
  -- cbstroke, fu_cbstroke,
  -- cbtia, fu_cbtia
  COALESCE(hyperten, fu_hyperten) AS hyperten,
  COALESCE(hypercho, fu_hypercho) AS hypercho,
  COALESCE(diabetes, fu_diabetes) AS diabetes,
  COALESCE(cbstroke, fu_cbstroke) AS cbstroke,
  COALESCE(cbtia,    fu_cbtia   ) AS cbtia
FROM public.a5
WHERE
  ptid      >= 'UM00000543'::text AND
  form_date >= '2017-03-15'::date
ORDER BY ptid, form_date;
