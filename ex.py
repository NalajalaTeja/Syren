jnj_od=union_df.alias("ud").join(jnjcalendar.alias('jc'),date_format(to_timestamp("ud.OPENED_DATE","yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),"yyyy-MM-dd") == to_date(col("jc.CALENDAR_DATE"), "yyyy-MM-dd"),"left").select(col('ud.*'),col('jc.fiscal_year_number'),col('jc.fiscal_month_number'),col('jc.calendar_month_start_date').alias('GREGORIAN_CAL_DATE'),col('jc.fiscal_calendar_month_start_date').alias('FISCAL_CAL_DATE'))

jnj_cd=jnj_od.alias("jo").join(jnjcalendar.alias('jc'),date_format(to_timestamp("jo.CLOSED_DATE","yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),"yyyy-MM-dd") == to_date(col("jc.CALENDAR_DATE"), "yyyy-MM-dd"),"left").select(col('jo.*'),col('jc.fiscal_month_year_name').alias('FISCAL_CLOSED_DATE'))

jnj_dd=jnj_cd.alias("jd").join(jnjcalendar.alias('jc'),date_format(to_timestamp("jd.DUE_DATE","yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),"yyyy-MM-dd") == to_date(col("jc.CALENDAR_DATE"), "yyyy-MM-dd"),"left").select(col('jd.*'),col('jc.fiscal_month_year_name').alias('FISCAL_DUE_DATE'),col('jc.calendar_month_start_date').alias('GREGORIAN_DUE_DATE'))

final_table=jnj_dd.withColumn("L2_CREATED_DATE",current_date())
final_table=final_table.withColumn("L2_UPDATED_DATE",current_date())
final_table.filter(final_table.AUDIT_RECORD_ID=='SA-001725').display()



