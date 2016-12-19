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


select st.* from thgsnf_praluent_survey_trn st join thgsnf_praluent_master_trn mt 
on mt.file_id = st.file_id and  mt.hib_id = st.hib_id and mt.valid = 1 ;


set serveroutput on;

begin
    MDB_FILE_LOADER_SP('spatel2@hibbertgroup.com','Praluent_Enrollment_Hibbert*');
end;



SNF_TOUJEO_TRN_PR_SP;


select s1.survey_name,
       s1.survey_description,
       s1.survey_id,
       s2.question_id,
       q1.question_alias,
       q1.question_text,
       s2.entry_sequence question_sort_order,
       s3.answer_id,
       s3.entry_sequence answer_sort_order,
       a1.answer_alias,
       a1.answer_text
  from hgappsnf.thgmdb_survey_dtl s1,
       hgappsnf.thgmdb_srvy_quest_xref s2,
       hgappsnf.thgmdb_srvy_ans_xref s3,
       hgappsnf.thgmdb_questions q1,
       hgappsnf.thgmdb_answers a1
where s1.survey_name = 'PraluentEnrollmentForm'
   and s1.survey_id = s2.survey_id
   and s1.survey_id = s3.survey_id
   and s2.question_id = s3.question_id
   and s2.question_id = q1.question_id
   and s3.answer_id = a1.answer_id
order by question_id
;