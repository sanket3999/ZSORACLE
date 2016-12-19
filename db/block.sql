select * from all_objects where object_name like upper('%mdb%%sp%') and object_type = 'PROCEDURE' ;

select * from thgmdb_file_xref where file_name_mask = 'Praluent_Enrollment_Hibbert*';
select * from thgmdb_file_source where file_name_mask = 'Praluent_Enrollment_Hibbert*';
select * from thgmdb_file_schedule where file_name_mask = 'Praluent_Enrollment_Hibbert*';


delete thg_activity_log where job_name = 'SNF_PRALUENT_TRN_LD_SP';
delete thgsnf_praluent_master_trn;
delete thgsnf_praluent_survey_trn;

select * from thg_activity_log where job_name = 'SNF_PRALUENT_TRN_LD_SP';
select * from thgsnf_praluent_master_trn;
select * from thgsnf_praluent_survey_trn;

set serveroutput on;
begin
    MDB_FILE_LOADER_SP('spatel2@hibbertgroup.com','Praluent_Enrollment_Hibbert*');
end;

