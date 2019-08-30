-- !preview conn=con

SELECT 
  *
FROM public.wtar
WHERE
  ptid      >= 'UM00000543'::text AND
  form_date >= '2017-03-15'::date
ORDER BY ptid, form_date;
