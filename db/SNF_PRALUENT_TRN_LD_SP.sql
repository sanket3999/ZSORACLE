CREATE OR REPLACE PROCEDURE HGAPPSNF.SNF_PRALUENT_TRN_LD_SP
( p_client_id                      in  varchar2,
  p_file_xref_id                   in  varchar2,
  p_file_type                      in  varchar2,
  p_vendor_id                      in  varchar2,
  p_ip_dir                         in  varchar2,
  p_ip_file                        in  varchar2,
  p_email_notify                   in  varchar2
)
as
-- --------------------------------------------------------------------------------------------------
--  This process load the data for the file pattern like Praluent_Enrollment_Hibbert_yyyymmdd.txt  --
--  placed in  
--  MODIFICATION HISTORY
--  Person             Date        PCR#          Comments
-- -----------------  ----------  ---------------------------------------------------------------- --
--  Sanket Patel       12/13/2016  161128144133  Creation of procedure 
-- --------------------------------------------------------------------------------------------------
  -- exception handlers.  
  proc_error                 exception;
  pragma exception_init     (proc_error,-20011);
  dml_error                  exception;
  pragma exception_init     (dml_error,-24381);
  -----------------------------------------------------------------------------------------------------------
  ip_file_handle             utl_file.file_type;
  ip_dir                     varchar2(100)                                      :=  null;
  ip_file                    varchar2(100)                                      :=  null;
  ip_record                  varchar2(32000)                                    :=  null;
  -----------------------------------------------------------------------------------------------------------
  -- tab, carriage return, and line feed.   
  v_tab                      varchar2(01)                                       :=  chr(09);
  v_carr_rtn                 varchar2(01)                                       :=  chr(13);
  v_line_feed                varchar2(01)                                       :=  chr(10);
  ------------------------------------------------------------------------------------------------------
  -- delimiter fields
  ip_fields                  hgappthg.THG_UTILS_PKG.varchar_tbl;
  db_delim                   varchar2(01)                                       := '|';
  ip_cols                    hgappthg.THG_UTILS_PKG.varchar_tbl;
  fld_cnt                    number  (05)                                       :=  0;
  -----------------------------------------------------------------------------------------------------------
  -- procedure variable fields. 
  db_log_id                  thgmdb_log_process_counts.log_id%type              :=  thg_job_log_id_seq.nextval;
  db_log_seq                 thgmdb_log_process_counts.log_seq%type             :=  0;
  db_log_table_id            thgmdb_log_process_counts.table_id%type            :=  null;
  db_log_table_name          thgmdb_db_table_xref.table_name%type               :=  null;
  db_log_desc                thgmdb_log_process_counts.description%type         :=  null;
  db_log_cnt                 thgmdb_log_process_counts.rec_cnt%type             :=  null;
  db_envir                   thg_activity_log.server%type                       :=  null;
  db_schema                  thg_activity_log.server_environment%type           :=  null;
  db_schema_client           thgmdb_code_description.description%type           :=  null;
  db_procedure_id            thgmdb_procedure_xref.procedure_id%type            :=  null;
  db_procedure_name          thgmdb_procedure_xref.procedure_name%type          :=  $$plsql_unit;
  db_column_id               thgmdb_db_columns_xref.column_id%type              :=  null;
  db_column_name             thgmdb_db_columns_xref.column_name%type            :=  null;
  db_rtn_code                number                                             :=  null;
  db_error_id                thgmdb_error_xref.error_id%type                    :=  null;
  db_error_desc              thgmdb_error_xref.error_desc%type                  :=  null;
  v_proc_msg                 varchar2(4000)                                     :=  null;
  v_proc_step_msg            varchar2(250)                                      :=  null;
  v_log_comments             varchar2(4000)                                     :=  null;
  v_email_subject            varchar2(200)                                      :=  null;
  v_email_message            varchar2(4000)                                     :=  null;
  start_date                 varchar2(19)                                       :=  to_char(sysdate,'mm/dd/yyyy hh24:mi:ss');
  end_date                   varchar2(19)                                       :=  null;
  proc_sysdate               date                                               :=  sysdate;
  trunc_proc_date            date                                               :=  trunc(sysdate);
  db_date                    date                                               :=  null;
  db_number                  number                                             :=  null;
  v_cnt                      number  (05)                                       :=  0; 
  dir_file                   varchar2(200)                                      :=  p_ip_dir||'/'||p_ip_file;
  -----------------------------------------------------------------------------------------------------------
  -- work fields.  
  v_pos                      number  (05)                                       :=  0;
  v_pos2                     number  (05)                                       :=  0;
  v_len                      number  (05)                                       :=  0;
  ip_recs                    number  (10)                                       :=  0;
  dr_recs                    number  (10)                                       :=  0;
  dup_recs                   number  (10)                                       :=  0;
  bulk_insert_cnt            number  (10)                                       :=  0;
  bulk_insert_max            number  (10)                                       :=  5000;
  db_file_xref_id            thgmdb_file_xref.file_xref_id%type                 :=  null;
  db_file_type               thgmdb_file_xref.file_type%type                    :=  null;
  db_file_name_mask          thgmdb_file_xref.file_name_mask%type               :=  null;
  db_file_id                 thgmdb_file_control.file_id%type                   :=  null;
  db_file_sub_id             thgmdb_file_control.file_sub_id%type               :=  null;
  db_file_date               thgmdb_file_control.file_date%type                 :=  null;
  db_expected_rec_cnt        thgmdb_file_control.expected_rec_cnt%type          :=  0;
  db_vendor_id               thgmdb_con_trans_dtl_xref.vendor_id%type           :=  0;
  fld_nbr                    number  (05)                                       :=  0;
  db_client_id               thgsnf_praluent_master_trn.client_id%type            :=  null;
  db_hib_id                  thgsnf_praluent_master_trn.hib_id%type               :=  null;
  db_hib_id_seq_1            thgsnf_praluent_master_trn.hib_id_seq%type           :=  null;
  db_hib_id_seq_2            thgsnf_praluent_master_trn.hib_id_seq%type           :=  0;
  hld_seq_num                varchar(20)                                        := null;
  -----------------------------------------------------------------------------------------------------------
  -- db records 
  r1_rec                     thgmdb_file_control%rowtype                        :=  null;
  -----------------------------------------------------------------------------------------------------------
  -- db record tables
 
  t1_table_id                thgmdb_db_table_xref.table_id%type                 :=  null;
  t1_table_name              thgmdb_db_table_xref.table_name%type               :=  upper('thgsnf_praluent_master_trn');
  t1_recs                    number  (10)                                       :=  0;
  t1                         number  (10)                                       :=  0;
  type t1t_rec  is table of  thgsnf_praluent_master_trn%rowtype                  index by binary_integer;
  t1_rec                     t1t_rec;
  
  t2_table_id                thgmdb_db_table_xref.table_id%type                 :=  null;
  t2_table_name              thgmdb_db_table_xref.table_name%type               :=  upper('thgsnf_praluent_survey_trn');
  t2_recs                    number  (10)                                       :=  0;
  t2                         number  (10)                                       :=  0;
  type t2t_rec  is table of  thgsnf_praluent_survey_trn%rowtype                  index by binary_integer;
  t2_rec                     t2t_rec;
  -----------------------------------------------------------------------------------------------------------
-- ********************************************************************************************************** 
--                                   S U B    P R O G R A M S          
-- ********************************************************************************************************** 

-------------------------------------------------------------------------------------------------------------
procedure proc_params
-------------------------------------------------------------------------------------------------------------
is
    sLoadDT             varchar2(100);
    sFileID             number;
begin

  begin
     db_client_id                                :=  to_number(nvl(trim(p_client_id),0));
  exception
     when others
     then db_client_id                           :=  -1;
  end;
  if db_client_id < 0
  then
     raise_application_error(-20011,'invalid client_id parameter: '||p_client_id);
  end if;

  begin
     db_file_xref_id                             :=  to_number(nvl(trim(p_file_xref_id),0));
  exception
     when others
     then db_file_xref_id                        :=  0;
  end;
  if db_file_xref_id < 1
  then
     raise_application_error(-20011,'invalid file_xref_id parameter: '||p_file_xref_id);
  end if;

  db_file_type                                   :=  trim(p_file_type);
  ip_dir                                         :=  trim(p_ip_dir);
  ip_file                                        :=  trim(p_ip_file);

  -- validate client_id and file_xref_id
  begin
     select file_name_mask
     into   db_file_name_mask
     from   thgmdb_file_xref
     where  client_id                             =  db_client_id
     and    file_type                             =  db_file_type
     and    file_xref_id                          =  db_file_xref_id;
  exception
     when others
     then db_file_name_mask                      := '?';
  end;
  --
  if ip_file  not like replace(db_file_name_mask,'*','%')
  then
     mdb_file_audit_pkg.insert_file_ctl(db_client_id, db_file_xref_id, db_file_type, ip_file,'N');
     raise_application_error(-20011,'invalid file name for client_id/file_xref_id parameters: '||p_file_xref_id);
  end if;

  -- check for duplicate file 
  if mdb_file_audit_pkg.is_file_loaded(ip_file, sLoadDT, sFileID ) = 'Y'
  then
     raise_application_error(-20011,'Duplicate file - '||ip_file||' loaded on '||sLoadDT||' file_id '||sFileID);
  else
     r1_rec                                      :=  null;
  end if;

  begin
     db_vendor_id                                :=  to_number(p_vendor_id);
  exception
     when others
     then raise_application_error(-20011,'invalid vendor_id parameter: '||p_vendor_id);
  end;

end proc_params;


-------------------------------------------------------------------------------------------------------------
procedure proc_file_name
-------------------------------------------------------------------------------------------------------------
is
begin

  ip_file_handle                                 :=  utl_file.fopen(ip_dir,ip_file,'R',32000);

  -- extract date from file name 
  v_pos                                          :=  instr (ip_file, '_', -1,1);
  v_pos2                                         :=  instr (ip_file, '.', -1,1)-1;
  if v_pos2 < v_pos
  then
     v_pos2                                      :=  length(ip_file);
  end if;
  v_len                                          :=  (v_pos2 - v_pos);
  if  v_pos > 0
  then 
      begin
         db_file_date                            :=  to_date(substr(ip_file,v_pos+1,v_len),'yyyymmddhh24miss');
      exception
         when others then
             mdb_file_audit_pkg.insert_file_ctl(db_client_id, db_file_xref_id, db_file_type, ip_file, 'D');  
            raise_application_error(-20011, 'Error unable to extract file date from file name');
      end;
  else
     mdb_file_audit_pkg.insert_file_ctl(db_client_id, db_file_xref_id, db_file_type, ip_file, 'D');  
     raise_application_error(-20011, 'Error unable to extract file date from file name');
  end if;

end proc_file_name;


-------------------------------------------------------------------------------------------------------------
procedure load_r1_rec
-------------------------------------------------------------------------------------------------------------
is
begin

  r1_rec                                         :=  null;
  r1_rec.client_id                               :=  db_client_id;
  r1_rec.file_id                                 :=  db_file_id;
  r1_rec.file_sub_id                             :=  db_file_sub_id;
  r1_rec.proc_date                               :=  proc_sysdate;
  r1_rec.file_xref_id                            :=  db_file_xref_id;
  r1_rec.file_type                               :=  db_file_type;
  r1_rec.file_name                               :=  ip_file;
  r1_rec.file_date                               :=  db_file_date;
  r1_rec.expected_rec_cnt                        :=  0;
  r1_rec.proc_rec_cnt                            :=  0;
  r1_rec.st1_err_rec_cnt                         :=  0;
  r1_rec.st2_err_rec_cnt                         :=  0;
  r1_rec.dropped_rec_cnt                         :=  0;
  r1_rec.status                                  := 'A';

  insert into thgmdb_file_control
  values r1_rec;
  commit;

end load_r1_rec;


-------------------------------------------------------------------------------------------------------------
procedure initial_process
-------------------------------------------------------------------------------------------------------------
is
begin

  -- activity_log
  mdb_global_pkg.activity_log_insert(db_log_id, 0, db_procedure_name, sysdate, 'JS', dir_file, 'job started');

  -- get db environment 
  select sys_context('userenv','db_name')        into db_envir  from dual;

  -- get schema 
  db_rtn_code                                    :=  mdb_global_pkg.get_schema(db_schema,db_schema_client);
  if db_rtn_code = -1
  then 
     raise_application_error(-20011,'Error getting schema from thgmdb_code_description table');
  end if;

  -- init messages
  v_proc_msg                                     := 'DB: '||db_envir||'   Schema: '||db_schema||' - '||db_schema_client
                                                 ||v_tab||v_carr_rtn||'Process: '||db_procedure_name;

  -- make sure this procedure isn't already running
  db_number                                      :=  mdb_global_pkg.is_proc_running(db_schema,db_procedure_name); 
  if db_number = -1
  then
     raise_application_error(-20011,'Error - Previous ('||db_schema||'.'||db_procedure_name||') has not Completed');
  end if;
  
  v_proc_step_msg                                := 'proc_params';
  proc_params;
  --
  v_proc_step_msg                                := 'proc_file_name';
  proc_file_name;
  --
  select thgmdb_file_id_seq.nextval            into  db_file_id     from dual;
  select thgmdb_file_sub_id_seq.nextval        into  db_file_sub_id from dual;
  --
  v_proc_step_msg                                := 'load_r1_rec';
  load_r1_rec;

end initial_process;    


-------------------------------------------------------------------------------------------------------------
procedure mdb_global_pkg_init
-------------------------------------------------------------------------------------------------------------
is
begin

  -- load mdb_global_pkg arrays
  db_number                                      :=  mdb_global_pkg.get_table_id(0,null);

  -- get db_procedure_id
  db_procedure_id                                :=  mdb_global_pkg.get_procedure_id(db_procedure_name);
  if db_procedure_id = -1
  then 
     raise_application_error(-20011,'Error getting procedure_id from thgmdb_procedure_xref table for '||db_procedure_name);
  end if;

  -- get t1_table_id
  t1_table_id                                    :=  mdb_global_pkg.get_table_id(1,t1_table_name);
  if t1_table_id = -1
  then 
     raise_application_error(-20011,'Error getting table_id from thgmdb_db_table_xref table for '||t1_table_name);
  end if;

  -- get t2_table_id
  t2_table_id                                    :=  mdb_global_pkg.get_table_id(1,t2_table_name);
  if t2_table_id = -1
  then 
     raise_application_error(-20011,'Error getting table_id from thgmdb_db_table_xref table for '||t2_table_name);
  end if;
  
  
end mdb_global_pkg_init;


-------------------------------------------------------------------------------------------------------------
procedure load_t1_rec
-------------------------------------------------------------------------------------------------------------
is
begin -- thgsnf_praluent_master_trn (01)

  fld_cnt                                        := ip_fields.count;
  if fld_cnt < 37 or fld_cnt > 38 --allow for trailing delimiter
  then
     raise_application_error(-20011,'invalid number of columns');
  end if;
  
  db_hib_id_seq_1                                :=  0;
  t1                                             :=  t1 + 1;
  t1_rec(t1)                                     :=  null;
  t1_rec(t1).client_id                           :=  db_client_id;
  t1_rec(t1).table_id                            :=  t1_table_id;
  t1_rec(t1).record_type                         := 'CON';
  t1_rec(t1).hib_id                              :=  db_hib_id;
  t1_rec(t1).hib_id_seq                          :=  db_hib_id_seq_1;
  t1_rec(t1).file_id                             :=  db_file_id;
  t1_rec(t1).load_date                           :=  proc_sysdate;
  t1_rec(t1).proc_date                           :=  null;   
  if ip_fields(01) = 1 then
  t1_rec(t1).status                              := 'N';
  else 
  t1_rec(t1).status                              := 'I'; --invalid record sent to the email fulfillment
  end if;
  t1_rec(t1).maint_type                          := 'I';
  begin
    fld_nbr    :=1;     t1_rec(t1).valid                    :=  ip_fields(01) ;
    fld_nbr    :=2;     t1_rec(t1).batch_id                 :=  ip_fields(02) ;
    fld_nbr    :=3;     t1_rec(t1).seq_num                  :=  ip_fields(03) ;
    fld_nbr    :=4;     t1_rec(t1).product_code             :=  ip_fields(04) ;
    fld_nbr    :=5;     t1_rec(t1).source_code              :=  ip_fields(05) ;
    fld_nbr    :=6;     t1_rec(t1).contact_date             :=  ip_fields(06) ;
    fld_nbr    :=7;     t1_rec(t1).referring_hcp            :=  ip_fields(07) ;
    fld_nbr    :=8;     t1_rec(t1).npi#                     :=  ip_fields(08) ;
    fld_nbr    :=9;     t1_rec(t1).hcp_street_address       :=  ip_fields(09) ;
    fld_nbr    :=10;    t1_rec(t1).company_name             :=  ip_fields(010);
    fld_nbr    :=11;    t1_rec(t1).hcp_city                 :=  ip_fields(011);
    fld_nbr    :=12;    t1_rec(t1).hcp_state                :=  ip_fields(012);
    fld_nbr    :=13;    t1_rec(t1).hcp_zip                  :=  ip_fields(013);
    fld_nbr    :=14;    t1_rec(t1).hcp_zip_4                :=  ip_fields(014);
    fld_nbr    :=15;    t1_rec(t1).hcp_phone                :=  ip_fields(015);
    fld_nbr    :=16;    t1_rec(t1).patient_prefix           :=  ip_fields(016);
    fld_nbr    :=17;    t1_rec(t1).patient_first_name       :=  ip_fields(017);
    fld_nbr    :=18;    t1_rec(t1).patient_middle_name      :=  ip_fields(018);
    fld_nbr    :=19;    t1_rec(t1).patient_last_name        :=  ip_fields(019);
    fld_nbr    :=20;    t1_rec(t1).patient_suffix           :=  ip_fields(020);
    fld_nbr    :=21;    t1_rec(t1).patient_address1         :=  ip_fields(021);
    fld_nbr    :=22;    t1_rec(t1).patient_address2         :=  ip_fields(022);
    fld_nbr    :=23;    t1_rec(t1).patient_city             :=  ip_fields(023);
    fld_nbr    :=24;    t1_rec(t1).patient_state            :=  ip_fields(024);
    fld_nbr    :=25;    t1_rec(t1).patient_zip              :=  ip_fields(025);
    fld_nbr    :=26;    t1_rec(t1).patient_zip_4            :=  ip_fields(026);
    fld_nbr    :=27;    t1_rec(t1).patient_phone            :=  ip_fields(027);
    fld_nbr    :=28;    t1_rec(t1).patient_dob              :=  ip_fields(028);
    fld_nbr    :=29;    t1_rec(t1).patient_email_address    :=  ip_fields(029);
    fld_nbr    :=30;    t1_rec(t1).date_of_birth            :=  ip_fields(030);
    fld_nbr    :=31;    t1_rec(t1).gender                   :=  ip_fields(031);
    fld_nbr    :=36;    t1_rec(t1).patient_sign             :=  ip_fields(036);
    fld_nbr    :=37;    t1_rec(t1).patient_date             :=  ip_fields(037);
    fld_nbr    :=38;    t1_rec(t1).print_name               :=  ip_fields(038);
  exception
  when others
  then raise_application_error(-20011,'invalid column length/type fld_nbr: '||fld_nbr||' value: '||ip_fields(fld_nbr));
  end;

end load_t1_rec;


---------------------------------------------------------------------------------------------------------------
procedure insert_t1_recs 
---------------------------------------------------------------------------------------------------------------
is
begin  --thgsnf_praluent_master_trn (01)

  bulk_insert_cnt                                :=  0;      
  commit;
  begin
     forall i in 1..t1_rec.count save exceptions
        insert into thgsnf_praluent_master_trn
        values t1_rec(i);
  exception
     when dml_error 
     then
        for e in 1..sql%bulk_exceptions.count
        loop 
           raise_application_error(-20011,'insert error ' ||t1_table_name
                                       || ' client_id: '  ||t1_rec(sql%bulk_exceptions(e).error_index).client_id
                                       || ' table_id: '   ||t1_rec(sql%bulk_exceptions(e).error_index).table_id
                                       || ' record_type: '||t1_rec(sql%bulk_exceptions(e).error_index).record_type
                                       || ' hib_id: '     ||t1_rec(sql%bulk_exceptions(e).error_index).hib_id
                                       || ' hib_id_seq: ' ||t1_rec(sql%bulk_exceptions(e).error_index).hib_id_seq
                                       || ' sqlerrm: '    ||sqlerrm(-sql%bulk_exceptions(e).error_code));
        end loop;
  end;
  t1_recs                                        :=  t1_recs + sql%rowcount;
  t1                                             :=  0;
  t1_rec.delete;
  commit;

end insert_t1_recs;

-------------------------------------------------------------------------------------------------------------
procedure load_t2_rec
-------------------------------------------------------------------------------------------------------------
is
begin --thgsnf_praluent_survey_trn (02)

  fld_cnt                                        := ip_fields.count;
--  if fld_cnt < 7 or fld_cnt > 8 --allow for trailing delimiter
--  then
--     raise_application_error(-20011,'invalid number of columns');
--  end if;

  --Question 1 Best time of day to be contacted ?  FIELD NBR 32
  db_hib_id_seq_2 := 1;
  
  t2                                             :=  t2 + 1;
  t2_rec(t2)                                     :=  null;
  t2_rec(t2).client_id                           :=  db_client_id;
  t2_rec(t2).table_id                            :=  t2_table_id;
  t2_rec(t2).record_type                         := 'CON';
  t2_rec(t2).hib_id                              :=  db_hib_id;
  
  t2_rec(t2).hib_id_seq                          :=  db_hib_id_seq_2;
  t2_rec(t2).file_id                             :=  db_file_id;
  t2_rec(t2).load_date                           :=  proc_sysdate;
  t2_rec(t2).PROC_DATE                           :=  null;   
  if ip_fields(01) = 1 then
  t2_rec(t2).status                              := 'N';
  else
  t2_rec(t2).status                              := 'I'; --invalid record sent to the email fulfillment 
  end if;
  t2_rec(t2).maint_type                          := 'I';
  begin
     fld_nbr                                     :=  1;
     t2_rec(t2).batch_id                         :=  ip_fields(02);
     fld_nbr                                     :=  2;
     t2_rec(t2).seq_num                          :=  ip_fields(03);
     fld_nbr                                     :=  3;
     t2_rec(t2).question_id                      :=  null;
     fld_nbr                                     :=  32;
     t2_rec(t2).answer_id                        :=  ip_fields(32);
     fld_nbr                                     :=  5;
     t2_rec(t2).open_ended_text_ind              :=  null;
     fld_nbr                                     :=  6;
     t2_rec(t2).open_ended_text_ans              :=  null;
     
  exception
  when others
  then raise_application_error(-20011,'invalid column length/type fld_nbr: '||fld_nbr||' value: '||ip_fields(fld_nbr));
  end;
  --
  
   --Question 2 Language Preference ? FIELD NBR 33
  t2                                             :=  t2 + 1;
  t2_rec(t2)                                     :=  null;
  t2_rec(t2).client_id                           :=  db_client_id;
  t2_rec(t2).table_id                            :=  t2_table_id;
  t2_rec(t2).record_type                         := 'CON';
  t2_rec(t2).hib_id                              :=  db_hib_id;
  db_hib_id_seq_2                                :=  db_hib_id_seq_2+1;
  t2_rec(t2).hib_id_seq                          :=  db_hib_id_seq_2;
  t2_rec(t2).file_id                             :=  db_file_id;
  t2_rec(t2).load_date                           :=  proc_sysdate;
  t2_rec(t2).PROC_DATE                           :=  null;   
  t2_rec(t2).status                              := 'N';
  t2_rec(t2).maint_type                          := 'I';
  begin
     fld_nbr                                     :=  1;
     t2_rec(t2).batch_id                         :=  ip_fields(02);
     fld_nbr                                     :=  2;
     t2_rec(t2).seq_num                          :=  ip_fields(03);
     fld_nbr                                     :=  3;
     t2_rec(t2).question_id                      :=  null;
     fld_nbr                                     :=  33;
     t2_rec(t2).answer_id                        :=  ip_fields(33);
     fld_nbr                                     :=  5;
     t2_rec(t2).open_ended_text_ind              :=  null;
     fld_nbr                                     :=  6;
     t2_rec(t2).open_ended_text_ans              :=  null;
     
  exception
  when others
  then raise_application_error(-20011,'invalid column length/type fld_nbr: '||fld_nbr||' value: '||ip_fields(fld_nbr));
  end;
  --
  
    --Question 3 I am over 18 years of age ?  FIELD NBR 34
  t2                                             :=  t2 + 1;
  t2_rec(t2)                                     :=  null;
  t2_rec(t2).client_id                           :=  db_client_id;
  t2_rec(t2).table_id                            :=  t2_table_id;
  t2_rec(t2).record_type                         := 'CON';
  t2_rec(t2).hib_id                              :=  db_hib_id;
  db_hib_id_seq_2                                :=  db_hib_id_seq_2+1;
  t2_rec(t2).hib_id_seq                          :=  db_hib_id_seq_2;
  t2_rec(t2).file_id                             :=  db_file_id;
  t2_rec(t2).load_date                           :=  proc_sysdate;
  t2_rec(t2).PROC_DATE                           :=  null;   
  t2_rec(t2).status                              := 'N';
  t2_rec(t2).maint_type                          := 'I';
  begin
     fld_nbr                                     :=  1;
     t2_rec(t2).batch_id                         :=  ip_fields(02);
     fld_nbr                                     :=  2;
     t2_rec(t2).seq_num                          :=  ip_fields(03);
     fld_nbr                                     :=  3;
     t2_rec(t2).question_id                      :=  null;
     fld_nbr                                     :=  34;
     t2_rec(t2).answer_id                        :=  ip_fields(34);
     fld_nbr                                     :=  5;
     t2_rec(t2).open_ended_text_ind              :=  null;
     fld_nbr                                     :=  6;
     t2_rec(t2).open_ended_text_ans              :=  null;
     
  exception
  when others
  then raise_application_error(-20011,'invalid column length/type fld_nbr: '||fld_nbr||' value: '||ip_fields(fld_nbr));
  end;
  --
    --Question 4 I am over 18 years of age ?  FIELD NBR 35
  t2                                             :=  t2 + 1;
  t2_rec(t2)                                     :=  null;
  t2_rec(t2).client_id                           :=  db_client_id;
  t2_rec(t2).table_id                            :=  t2_table_id;
  t2_rec(t2).record_type                         := 'CON';
  t2_rec(t2).hib_id                              :=  db_hib_id;
  db_hib_id_seq_2                                :=  db_hib_id_seq_2+1;
  t2_rec(t2).hib_id_seq                          :=  db_hib_id_seq_2;
  t2_rec(t2).file_id                             :=  db_file_id;
  t2_rec(t2).load_date                           :=  proc_sysdate;
  t2_rec(t2).PROC_DATE                           :=  null;   
  t2_rec(t2).status                              := 'N';
  t2_rec(t2).maint_type                          := 'I';
  begin
     fld_nbr                                     :=  1;
     t2_rec(t2).batch_id                         :=  ip_fields(02);
     fld_nbr                                     :=  2;
     t2_rec(t2).seq_num                          :=  ip_fields(03);
     fld_nbr                                     :=  3;
     t2_rec(t2).question_id                      :=  null;
     fld_nbr                                     :=  35;
     t2_rec(t2).answer_id                        :=  ip_fields(35);
     fld_nbr                                     :=  5;
     t2_rec(t2).open_ended_text_ind              :=  null;
     fld_nbr                                     :=  6;
     t2_rec(t2).open_ended_text_ans              :=  null;
     
  exception
  when others
  then raise_application_error(-20011,'invalid column length/type fld_nbr: '||fld_nbr||' value: '||ip_fields(fld_nbr));
  end;
 
end load_t2_rec;


---------------------------------------------------------------------------------------------------------------
procedure insert_t2_recs 
---------------------------------------------------------------------------------------------------------------
is
begin  --thgsnf_praluent_survey_trn (02)

  bulk_insert_cnt                                :=  0;      
  commit;
  begin
     forall i in 1..t2_rec.count save exceptions
        insert into thgsnf_praluent_survey_trn
        values t2_rec(i);
  exception
     when dml_error 
     then
        for e in 1..sql%bulk_exceptions.count
        loop 
           raise_application_error(-20011,'insert error ' ||t2_table_name
                                       || ' client_id: '  ||t2_rec(sql%bulk_exceptions(e).error_index).client_id
                                       || ' table_id: '   ||t2_rec(sql%bulk_exceptions(e).error_index).table_id
                                       || ' record_type: '||t2_rec(sql%bulk_exceptions(e).error_index).record_type
                                       || ' hib_id: '     ||t2_rec(sql%bulk_exceptions(e).error_index).hib_id
                                       || ' hib_id_seq: ' ||t2_rec(sql%bulk_exceptions(e).error_index).hib_id_seq
                                       || ' sqlerrm: '    ||sqlerrm(-sql%bulk_exceptions(e).error_code));
        end loop;
  end;
  t2_recs                                        :=  t2_recs + sql%rowcount;
  t2                                             :=  0;
  t2_rec.delete;
  commit;

end insert_t2_recs;

---------------------------------------------------------------------------------------------------------------
procedure proc_ip_file 
---------------------------------------------------------------------------------------------------------------
is
begin

  begin
     v_proc_step_msg                             := 'utl_file.get_line loop';        
     -- process input records.  
     loop
        utl_file.get_line(ip_file_handle,ip_record);
        --
    if trim(ip_record) is null or instr(lower(ip_record),'valid') >0 
       then
           dr_recs                                    :=  dr_recs + 1;
       else

   ip_recs                                    :=  ip_recs + 1;
   ip_fields                                  :=  hgappthg.THG_UTILS_PKG.split_string(ip_record,db_delim);
       

           
               db_hib_id     :=  thgmdb_hib_id_seq.nextval;
              
            
--            if nvl(ip_fields(03),'<null>') <>  hld_seq_num 
--            or ip_fields(01)  = '01'  then
--               db_hib_id     := thgmdb_hib_id_seq.nextval;
--               hld_seq_num   :=  ip_fields(03);
--               db_hib_id_seq_2    :=  0;
--            end if;
           
           --
            v_proc_step_msg           := 'load_t1_rec';        
            load_t1_rec;
           --
            v_proc_step_msg           := 'load_t2_rec';        
            load_t2_rec;
         --
   end if;
        -- 
        bulk_insert_cnt                                :=  bulk_insert_cnt + 1;      
           if bulk_insert_cnt >= bulk_insert_max
             then
           v_proc_step_msg                             := 'insert_t1_recs';
           insert_t1_recs;
           --
           v_proc_step_msg                             := 'insert_t2_recs';
           insert_t2_recs;
           --           
           end if;
        -- 
     end loop;

  exception
     when no_data_found then
          v_proc_step_msg                               := 'eof insert_t1_recs';
          insert_t1_recs;
          --
          v_proc_step_msg                               := 'eof insert_t2_recs';
          insert_t2_recs;
          --
          -- close file.  
          if  utl_file.is_open(ip_file_handle) 
          then
              utl_file.fclose(ip_file_handle);
          end if;
          --
          begin
              utl_file.fremove(ip_dir, ip_file);
          exception
              when others then null;
          end;
  end;
  
end proc_ip_file;


---------------------------------------------------------------------------------------------------------------
procedure purge_process 
---------------------------------------------------------------------------------------------------------------
is
begin

     delete from thgmdb_file_control
     where  file_id                               =  db_file_id;
     commit;
     --
     delete from thgsnf_praluent_master_trn
     where  file_id                               =  db_file_id;
     commit;
     --
     delete from thgsnf_praluent_survey_trn
     where  file_id                               =  db_file_id;
     commit;
end purge_process;


---------------------------------------------------------------------------------------------------------------
procedure log_process_cnts
---------------------------------------------------------------------------------------------------------------
is
begin  --hgappbi.thgmdb_log_process_counts

  db_log_seq                                     :=  db_log_seq + 1;
  db_rtn_code                                    :=  mdb_global_pkg.insert_log_process_counts(db_log_id, db_log_seq, db_file_id, db_procedure_id, db_log_table_id, 
                                                                                              db_log_desc, db_log_cnt, proc_sysdate);
  if db_rtn_code = -1
  then 
     raise_application_error(-20011,'Error inserting into thgmdb_log_process_counts table');
  end if;

end log_process_cnts;


---------------------------------------------------------------------------------------------------------------
procedure close_process
---------------------------------------------------------------------------------------------------------------
is
begin
  db_log_table_id                                :=  0;
  db_log_desc                                    :=  upper('input records');
  db_log_cnt                                     :=  ip_recs;
  log_process_cnts;

  if dr_recs > 0
  then
     db_log_table_id                             :=  0;
     db_log_desc                                 :=  upper('blank records (dropped)');
     db_log_cnt                                  :=  dr_recs;
     log_process_cnts;
  end if;

  if t1_recs > 0
  then
     db_log_table_id                             :=  t1_table_id;
     db_log_desc                                 :=  upper('inserts');
     db_log_cnt                                  :=  t1_recs;
     log_process_cnts;
  end if;
  
    if t2_recs > 0
  then
     db_log_table_id                             :=  t2_table_id;
     db_log_desc                                 :=  upper('inserts');
     db_log_cnt                                  :=  t2_recs;
     log_process_cnts;
  end if;

  v_log_comments                                 := 'file_id: '||db_file_id||' file_sub_id: '||db_file_sub_id;
  mdb_global_pkg.activity_log_update(db_log_id, 0, 'JC',v_log_comments);

end close_process;


-------------------------------------------------------------------------------------------------------------
procedure update_r1_rec
-------------------------------------------------------------------------------------------------------------
is
begin

  update thgmdb_file_control
  set    proc_rec_cnt                             =  ip_recs,
         dropped_rec_cnt                          =  dr_recs,
         expected_rec_cnt                         =  db_expected_rec_cnt,
         status                                   = 'L'
   where file_id                                  =  db_file_id
   and   file_sub_id                              =  db_file_sub_id;

end update_r1_rec;


-- **********************************************************************************************************
--                             M A I N   P R O C E S S I N G   A R E A  
-- **********************************************************************************************************

BEGIN

  v_proc_step_msg                                := 'initial_process';        
  initial_process; 
  --
  v_proc_step_msg                                := 'mdb_global_pkg_init';        
  mdb_global_pkg_init;
  --
  v_proc_step_msg                                := 'proc_ip_file';
  proc_ip_file;
  --
  v_proc_step_msg                                := 'close_process';
  close_process;
  --
  v_proc_step_msg                                := 'update_r1_rec';
  update_r1_rec;
  --
  v_proc_step_msg                                := 'SNF_TOUJEO_TRN_PR_SP';
  SNF_TOUJEO_TRN_PR_SP (p_vendor_id, p_email_notify);

-----------------------------------------------------------------------------------------
EXCEPTION
-----------------------------------------------------------------------------------------
  when no_data_found then null;
  
  when others then
       -- purge this run 
       purge_process;
       --
       -- close file.  
       if  utl_file.is_open(ip_file_handle) 
       then
           utl_file.fclose(ip_file_handle);
       end if;
       --
       v_log_comments                            := 'Error: file_id: '||db_file_id||' file_sub_id: '||db_file_sub_id||' ip_rec# '||ip_recs||' - '||sqlerrm||', Proc Step: '||v_proc_step_msg;
       mdb_global_pkg.activity_log_update(db_log_id, 0, 'JE', v_log_comments);
       --
       v_email_subject                           :=  nvl(db_schema,'?')||'.'||db_procedure_name;
       end_date                                  :=  to_char(sysdate,'mm/dd/yyyy hh24:mi:ss');
       v_email_message                           :=  v_proc_msg 
                              ||v_tab||v_carr_rtn||  null
                              ||v_tab||v_carr_rtn|| 'file_id....: '||db_file_id
                              ||v_tab||v_carr_rtn|| 'file_sub_id: '||db_file_sub_id
                              ||v_tab||v_carr_rtn|| 'ip_file....: '||dir_file
                              ||v_tab||v_carr_rtn|| 'ip_rec#....: '||ip_recs
                              ||v_tab||v_carr_rtn|| 'Error......: '||sqlerrm
                              ||v_tab||v_carr_rtn|| 'Proc Step..: '||v_proc_step_msg
                              ||v_tab||v_carr_rtn||  null
                              ||v_tab||v_carr_rtn|| 'Start Date.: '||start_date||' End Date: '||end_date;
       --
       mail_files_sp('oracle',
                      p_email_notify,
                     'Error - '||v_email_subject,
                      v_email_message, 
                      32000);

END SNF_PRALUENT_TRN_LD_SP;
/
