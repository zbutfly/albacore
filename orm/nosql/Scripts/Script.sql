SELECT ID, USER_ID, NAME, "TYPE", ADDRESS, STATUS, ADD_TIME, ADD_COMMENT
FROM DATAGGR.USER_APP;
DELETE FROM user_app WHERE id = '549589d00c5f67c7a9455c8d'

select * from (
	select TMP_A__.*, rownum row_num__ from (
		SELECT app_id,model_id FROM app_model_rel WHERE app_id='549695cb445fbaf7e16008fd'
	) TMP_A__ 
) TMP_B__ where TMP_B__.ROW_NUM__ between 1 and 5 

SELECT app_id,model_id FROM app_model_rel WHERE app_id='549695cb445fbaf7e16008fd'

