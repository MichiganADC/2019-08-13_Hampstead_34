-- !preview conn=con

-- madc_integ
--   public.
--     d1.
--       normcog, fu_normcog
--       demented, fu_demented
--       alzdis, fu_alzdis
--       alzdisif, fu_alzdisif

SELECT 
  ptid, form_date, 
  COALESCE(normcog,  fu_normcog ) AS normcog,
  COALESCE(demented, fu_demented) AS demented,
  COALESCE(alzdis,   fu_alzdis  ) AS alzdis,
  COALESCE(alzdisif, fu_alzdisif) AS alzdisif
FROM public.d1
WHERE
  ptid      >= 'UM00000543'::text AND
  form_date >= '2017-03-15'::date
ORDER BY ptid, form_date;
