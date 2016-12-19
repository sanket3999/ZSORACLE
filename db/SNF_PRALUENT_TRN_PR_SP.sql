CREATE OR REPLACE PROCEDURE HGAPPSNF.SNF_PRALUENT_TRN_PR_SP
(  
  p_vendor_id                in  varchar2,
  p_email_notify             in  varchar2
)
as

/*===========================================================================================================
    MODIFICATION HISTORY
    Person             Date        PCR#          Comments
    -----------------  ----------  ------------  ------------------------------------------------------------
    M. Lee             02/08/2016  160112131944  Creation of procedure
                                                 Input file has multiple record types, we assign a hib_id to
                                                 a group of records when the record_type changes or when the 
                                                 seq_num changes.  In this step we validate there is a "01" 
                                                 and at least one "02" record to each "group" of records.  
  ===========================================================================================================*/
  -- exception handlers.  
  proc_error                 exception;
  pragma exception_init     (proc_error,-20011);
  dml_error                  exception;
  pragma exception_init     (dml_error,-24381);
  -----------------------------------------------------------------------------------------------------------
  -- tab, carriage return, and line feed.   
  v_tab                      varchar2(01)                                       :=  chr(09);
  v_carr_rtn                 varchar2(01)                                       :=  chr(13);
  v_line_feed                varchar2(01)                                       :=  chr(10);
  ------------------------------------------------------------------------------------------------------
  --unstring delimited fields
  ip_fields                  hgappthg.THG_UTILS_PKG.varchar_tbl;
  fld_cnt                    number  (10)                                       :=  0;
  db_delim                   varchar2(01)                                       := '|';
  ------------------------------------------------------------------------------------------------------
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
  dir_file                   varchar2(200)                                      :=  null;
  -----------------------------------------------------------------------------------------------------------
  -- work fields.  
  error_sw                   varchar2(01)                                       := 'N';
  db_file_id                 thgmdb_file_control.file_id%type                   :=  0;
  db_vendor_id               thgmdb_hcp_trans_dtl_xref.vendor_id%type           :=  0;
  -----------------------------------------------------------------------------------------------------------
  -- db record
  type updt_record is record (
       client_id             thgsnf_toujeo_master_trn.client_id%type, 
       table_id              thgsnf_toujeo_master_trn.table_id%type, 
       record_type           thgsnf_toujeo_master_trn.record_type%type, 
       hib_id                thgsnf_toujeo_master_trn.hib_id%type, 
       hib_id_seq            thgsnf_toujeo_master_trn.hib_id_seq%type,
       status                thgsnf_toujeo_master_trn.status%type
       );
  ud_rec                     updt_record;
  -----------------------------------------------------------------------------------------------------------
  -- db record tables
  t1_table_id                thgmdb_db_table_xref.table_id%type                 :=  null;
  t1_table_name              thgmdb_db_table_xref.table_name%type               :=  upper('thgsnf_toujeo_master_trn');
  t1_recs                    number  (10)                                       :=  0;
  t1                         number  (10)                                       :=  0;
  type t1t_rec is table of   thgsnf_toujeo_master_trn%rowtype                   index by binary_integer;
  t1_rec                     t1t_rec;
  --
  t2_table_id                thgmdb_db_table_xref.table_id%type                 :=  null;
  t2_table_name              thgmdb_db_table_xref.table_name%type               :=  upper('thgsnf_toujeo_survey_trn');
  t2_recs                    number  (10)                                       :=  0;
  t2                         number  (10)                                       :=  0;
  type t2t_rec is table of   thgsnf_toujeo_survey_trn%rowtype                   index by binary_integer;
  t2_rec                     t2t_rec;
  --
  e1_hib_id                  thgmdb_error_dtl.hib_id%type                       :=  null;
  e1_hib_id_seq              thgmdb_error_dtl.hib_id_seq%type                   :=  null;
  e1_source_table_id         thgmdb_error_dtl.source_table_id%type              :=  null;
  e1_target_table            thgmdb_error_dtl.target_table%type                 :=  null;
  --
  e1_table_id                thgmdb_db_table_xref.table_id%type                 :=  null;
  e1_table_name              thgmdb_db_table_xref.table_name%type               :=  upper('thgmdb_error_dtl');
  e1_recs                    number  (10)                                       :=  0;
  e1                         number  (10)                                       :=  0;
  type e1t_rec  is table of  thgmdb_error_dtl%rowtype                           index by binary_integer;
  e1_rec                     e1t_rec;
  ------------------------------------------------------------------------------------------------------
  cursor c1_cur is 
  select s.hib_id as s_hib_id,
         m.*
  from   thgsnf_toujeo_master_trn          m 
  left outer join (select unique hib_id from thgsnf_toujeo_survey_trn) s
  on   s.hib_id                         =  m.hib_id
  where  status = 'N';
  --
  c1_recs                    number  (10)                                       :=  0;
  c1_row_cnt                 number  (10)                                       :=  0;
  c1                         number  (10)                                       :=  0;
  type  c1_table is table of c1_cur%rowtype                                     index by pls_integer;
  c1_rec                     c1_table;
  ------------------------------------------------------------------------------------------------------
  cursor c2_cur is 
  select m.hib_id as m_hib_id,
         s.*
  from   thgsnf_toujeo_survey_trn          s 
  left outer join (select unique hib_id from thgsnf_toujeo_master_trn) m
  on   m.hib_id                         =  s.hib_id
  where  status = 'N';
  --
  c2_recs                    number  (10)                                       :=  0;
  c2_row_cnt                 number  (10)                                       :=  0;
  c2                         number  (10)                                       :=  0;
  type  c2_table is table of c2_cur%rowtype                                     index by pls_integer;
  c2_rec                     c2_table;
  ------------------------------------------------------------------------------------------------------
  cursor c3_cur is 
  with trns as
  (select client_id, table_id, record_type, hib_id, hib_id_seq, ip_record_type
   from   thgsnf_toujeo_master_trn
   where status                         = 'PR'
   union 
   select client_id, table_id, record_type, hib_id, hib_id_seq, ip_record_type
   from   thgsnf_toujeo_survey_trn
   where status                         = 'PR')
  select trns.*
  from   trns,
         (select unique hib_id from thgmdb_error_dtl
          where source_table_id in (t1_table_id, t2_table_id)) e
  where  trns.hib_id                   =  e.hib_id;
  --
  c3_recs                    number  (10)                                       :=  0;
  c3_row_cnt                 number  (10)                                       :=  0;
  c3                         number  (10)                                       :=  0;
  type  c3_table is table of c3_cur%rowtype                                     index by pls_integer;
  c3_rec                     c3_table;
  ------------------------------------------------------------------------------------------------------
  --  a1 associative array 
  cursor a1_cur is
  select a.answer_alias
  from   thgmdb_srvy_ans_xref         ax,
         thgmdb_answers               a,
         thgmdb_survey_dtl            s
  where  lower(s.survey_name)      = 'toujeoivrla'
  and    s.survey_id               =  ax.survey_id
  and    ax.answer_id              =  a.answer_id;
  --
  a1_row_cnt                    number  (10)                                    :=  0;
  a1                            number  (10)                                    :=  0;
  type a1_cur_table is table of a1_cur%rowtype                                  index by binary_integer;
  a1_cur_rec                    a1_cur_table;
  --
  a1_key                        varchar2(255);
  type a1_table is table of     a1_cur%rowtype                                  index by varchar2(255);
  a1_rec                        a1_table;
  ------------------------------------------------------------------------------------------------------
-- ********************************************************************************************************** 
--                                   S U B    P R O G R A M S          
-- ********************************************************************************************************** 

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
  
  begin
     db_vendor_id                                :=  to_number(p_vendor_id);
  exception
     when others
     then raise_application_error(-20011,'invalid vendor_id parameter: '||p_vendor_id);
  end;

end initial_process;    


-------------------------------------------------------------------------------------------------------------
procedure mdb_global_pkg_init
-------------------------------------------------------------------------------------------------------------
is
begin

  -- load mdb_global_pkg arrays
  db_number                                      :=  mdb_global_pkg.get_table_id(0,null);
  db_number                                      :=  mdb_global_pkg.get_column_id(0,null);
  db_number                                      :=  mdb_global_pkg.get_error_id(0,null);

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

  -- get e1_table_id
  e1_table_id                                    :=  mdb_global_pkg.get_table_id(1,e1_table_name);
  if e1_table_id = -1
  then 
     raise_application_error(-20011,'Error getting table_id from thgmdb_db_table_xref table for '||e1_table_name);
  end if;

end mdb_global_pkg_init;


  ---------------------------------------------------------------------------------------------------------------
  PROCEDURE load_a1_array
  ---------------------------------------------------------------------------------------------------------------
  is
  begin -- thgmdb_type_desc

    a1_rec.delete;
    begin
       a1_row_cnt                                  :=  0;
       open a1_cur;
       loop 
          fetch a1_cur 
          bulk collect 
          into a1_cur_rec limit 1000;
          exit when a1_row_cnt                      =  a1_cur%rowcount;
          a1_row_cnt                               :=  a1_cur%rowcount;
          --  process bulk collected records. 
          for a1_idx in 1..a1_cur_rec.count
          loop
             a1                                    :=  a1_idx;
             a1_key                                :=  lower(a1_cur_rec(a1).answer_alias);
             a1_rec(a1_key)                        :=  a1_cur_rec(a1); 
          end loop;
       end loop;
       close a1_cur;
       --
       a1_cur_rec.delete;
    exception
       when others
       then null;
    end;

end load_a1_array;

---------------------------------------------------------------------------------------------------------------
PROCEDURE load_e1_rec
---------------------------------------------------------------------------------------------------------------
is
begin -- thgmdb_error_dtl

  db_column_id                                :=  mdb_global_pkg.get_column_id(1,UPPER(db_column_name));
  if db_column_id = -1
  then 
     raise_application_error(-20011,'Error getting column_id from thgmdb_db_columns_xref table for '||db_column_name);
  end if;

  db_error_id                                :=  mdb_global_pkg.get_error_id(1,UPPER(db_error_desc));
  if db_error_id = -1
  then 
     raise_application_error(-20011,'Error getting error_id from thgmdb_error_xref table for '||db_error_desc);
  end if;

  error_sw                                       := 'Y';
  e1                                             :=  e1 + 1;
  e1_rec(e1).hib_id                              :=  e1_hib_id;
  e1_rec(e1).hib_id_seq                          :=  e1_hib_id_seq;
  e1_rec(e1).source_table_id                     :=  e1_source_table_id;
  e1_rec(e1).column_id                           :=  db_column_id;
  e1_rec(e1).error_id                            :=  db_error_id;
  e1_rec(e1).procedure_id                        :=  db_procedure_id;
  e1_rec(e1).target_table                        :=  e1_target_table;
  e1_rec(e1).load_date                           :=  proc_sysdate;

end load_e1_rec;


---------------------------------------------------------------------------------------------------------------
procedure insert_e1_recs 
---------------------------------------------------------------------------------------------------------------
is
begin

  commit;
  begin
     forall i in 1..e1_rec.count save exceptions
        insert into thgmdb_error_dtl
        values e1_rec(i);
  exception
    when dml_error
     then
        for e in 1..sql%bulk_exceptions.count
        loop 
           if  sqlerrm(-sql%bulk_exceptions(e).error_code)  = 'ORA-00001: unique constraint (.) violated'
           then
               null;
           else
               raise_application_error(-20011,'insert error '      ||e1_table_name
                                           || ' hib_id: '          ||e1_rec(sql%bulk_exceptions(e).error_index).hib_id
                                           || ' source_table_id: ' ||e1_rec(sql%bulk_exceptions(e).error_index).source_table_id
                                           || ' column_id: '       ||e1_rec(sql%bulk_exceptions(e).error_index).column_id
                                           || ' error_id: '        ||e1_rec(sql%bulk_exceptions(e).error_index).error_id
                                           || ' sqlerrm: '         ||sqlerrm(-sql%bulk_exceptions(e).error_code));
           end if;
        end loop;
  end;
  e1_recs                                        :=  e1_recs + e1_rec.count;
  --
  e1                                             :=  0;
  e1_rec.delete;
  commit;

end insert_e1_recs;


-------------------------------------------------------------------------------------------------------------
procedure load_t1_rec
-------------------------------------------------------------------------------------------------------------
is
begin -- thgsnf_toujeo_master_trn

  t1                                             :=  t1 + 1;
  t1_rec(t1)                                     :=  null;
  t1_rec(t1).client_id                           :=  ud_rec.client_id;
  t1_rec(t1).table_id                            :=  ud_rec.table_id;
  t1_rec(t1).record_type                         :=  ud_rec.record_type;
  t1_rec(t1).hib_id                              :=  ud_rec.hib_id;
  t1_rec(t1).hib_id_seq                          :=  ud_rec.hib_id_seq;
  t1_rec(t1).status                              :=  ud_rec.status;

end load_t1_rec;


---------------------------------------------------------------------------------------------------------------
procedure update_t1_recs 
---------------------------------------------------------------------------------------------------------------
is
begin

  commit;
  begin
     forall i in 1..t1_rec.count save exceptions
        update thgsnf_toujeo_master_trn
        set    status                             =  t1_rec(i).status
        where  client_id                          =  t1_rec(i).client_id
        and    table_id                           =  t1_rec(i).table_id
        and    record_type                        =  t1_rec(i).record_type
        and    hib_id                             =  t1_rec(i).hib_id
        and    hib_id_seq                         =  t1_rec(i).hib_id_seq;
  exception
     when dml_error 
     then
        for e in 1..sql%bulk_exceptions.count
        loop 
           raise_application_error(-20011,'update error ' ||t1_table_name
                                       || ' client_id: '  ||t1_rec(sql%bulk_exceptions(e).error_index).client_id
                                       || ' table_id: '   ||t1_rec(sql%bulk_exceptions(e).error_index).table_id
                                       || ' record_type: '||t1_rec(sql%bulk_exceptions(e).error_index).record_type
                                       || ' hib_id: '     ||t1_rec(sql%bulk_exceptions(e).error_index).hib_id
                                       || ' hib_id_seq: ' ||t1_rec(sql%bulk_exceptions(e).error_index).hib_id_seq
                                       || ' sqlerrm: '    ||sqlerrm(-sql%bulk_exceptions(e).error_code));
        end loop;
  end;
  t1_recs                                       :=  t1_recs + t1_rec.count;
  commit;
  -- initialize 
  t1                                            :=  0;
  t1_rec.delete;

end update_t1_recs;


-------------------------------------------------------------------------------------------------------------
procedure load_t2_rec
-------------------------------------------------------------------------------------------------------------
is
begin -- thgsnf_toujeo_survey_trn

  t2                                             :=  t2 + 1;
  t2_rec(t2)                                     :=  null;
  t2_rec(t2).client_id                           :=  ud_rec.client_id;
  t2_rec(t2).table_id                            :=  ud_rec.table_id;
  t2_rec(t2).record_type                         :=  ud_rec.record_type;
  t2_rec(t2).hib_id                              :=  ud_rec.hib_id;
  t2_rec(t2).hib_id_seq                          :=  ud_rec.hib_id_seq;
  t2_rec(t2).status                              :=  ud_rec.status;

end load_t2_rec;


---------------------------------------------------------------------------------------------------------------
procedure update_t2_recs 
---------------------------------------------------------------------------------------------------------------
is
begin

  commit;
  begin
     forall i in 1..t2_rec.count save exceptions
        update thgsnf_toujeo_survey_trn
        set    status                             =  t2_rec(i).status
        where  client_id                          =  t2_rec(i).client_id
        and    table_id                           =  t2_rec(i).table_id
        and    record_type                        =  t2_rec(i).record_type
        and    hib_id                             =  t2_rec(i).hib_id
        and    hib_id_seq                         =  t2_rec(i).hib_id_seq;
  exception
     when dml_error 
     then
        for e in 1..sql%bulk_exceptions.count
        loop 
           raise_application_error(-20011,'update error ' ||t2_table_name
                                       || ' client_id: '  ||t2_rec(sql%bulk_exceptions(e).error_index).client_id
                                       || ' table_id: '   ||t2_rec(sql%bulk_exceptions(e).error_index).table_id
                                       || ' record_type: '||t2_rec(sql%bulk_exceptions(e).error_index).record_type
                                       || ' hib_id: '     ||t2_rec(sql%bulk_exceptions(e).error_index).hib_id
                                       || ' hib_id_seq: ' ||t2_rec(sql%bulk_exceptions(e).error_index).hib_id_seq
                                       || ' sqlerrm: '    ||sqlerrm(-sql%bulk_exceptions(e).error_code));
        end loop;
  end;
  t2_recs                                       :=  t2_recs + t2_rec.count;
  commit;
  -- initialize 
  t2                                            :=  0;
  t2_rec.delete;

end update_t2_recs;


-------------------------------------------------------------------------------------------------------------
procedure validate_c1_rec
-------------------------------------------------------------------------------------------------------------
is
begin -- thgsnf_toujeo_master_trn

  error_sw                                       := 'N';
  e1_hib_id                                      :=  c1_rec(c1).hib_id;
  e1_hib_id_seq                                  :=  c1_rec(c1).hib_id_seq;
  e1_source_table_id                             :=  t1_table_id;
  e1_target_table                                :=  t1_table_id;
  ------------------------------------------------------------------------------
  db_column_name                                 := 'hib_id';
  --
  if c1_rec(c1).s_hib_id is null
  then
     db_error_desc                               := 'not on '||t2_table_name; 
     load_e1_rec;
  end if;
  ------------------------------------------------------------------------------
  db_column_name                                 := 'first/last name';
  --
  if c1_rec(c1).first_name||c1_rec(c1).last_name is null
  then
     db_error_desc                               := 'can not be null'; 
     load_e1_rec;
  end if;
  ------------------------------------------------------------------------------
  db_column_name                                 := 'first/last name';
  --
  if c1_rec(c1).first_name||c1_rec(c1).last_name is null
  then
     db_error_desc                               := 'can not be null'; 
     load_e1_rec;
  end if;
  ------------------------------------------------------------------------------
  db_column_name                                 := 'contact information';
  --
  if  c1_rec(c1).primary_phone||c1_rec(c1).secondary_phone is null
  and c1_rec(c1).email_address is null
  and ((c1_rec(c1).address1 is null) or
       (c1_rec(c1).city||c1_rec(c1).zip is null) or
       (c1_rec(c1).state||c1_rec(c1).zip is null))
  then
     db_error_desc                               := 'can not be null'; 
     load_e1_rec;
  end if;
  ------------------------------------------------------------------------------

end validate_c1_rec;


--------------------------------------------------------------------------------------------------------
PROCEDURE proc_c1_rec
--------------------------------------------------------------------------------------------------------
is
begin 

  ud_rec                                         :=  null;
  ud_rec.client_id                               :=  c1_rec(c1).client_id;
  ud_rec.table_id                                :=  c1_rec(c1).table_id;
  ud_rec.record_type                             :=  c1_rec(c1).record_type;
  ud_rec.hib_id                                  :=  c1_rec(c1).hib_id;
  ud_rec.hib_id_seq                              :=  c1_rec(c1).hib_id_seq;
  ud_rec.status                                  :=  c1_rec(c1).status;
  if error_sw = 'Y'
  then
     ud_rec.status                               := 'ERR';
  else
     ud_rec.status                               := 'PR';
  end if;

  v_proc_step_msg                                := 'load_t1_rec';
  load_t1_rec;

end proc_c1_rec;


--------------------------------------------------------------------------------------------------------
PROCEDURE proc_c1_cur
--------------------------------------------------------------------------------------------------------
is
begin 

  c1_row_cnt                                     :=  0;
  open c1_cur;
  loop 
     -- bulk collect records from cursor.   
     fetch c1_cur
     bulk collect 
     into c1_rec limit 1000;
     exit when c1_row_cnt                         =  c1_cur%rowcount;
     c1_row_cnt                                  :=  c1_cur%rowcount;
     --
     for c1_idx in 1..c1_rec.count
     loop
        c1                                       :=  c1_idx;
        c1_recs                                  :=  c1_recs + 1;
        --
        v_proc_step_msg                          := 'validate_c1_rec';
        validate_c1_rec;
        --
        v_proc_step_msg                          := 'proc_c1_rec';
        proc_c1_rec;
        --
     end loop;
     --
     v_proc_step_msg                             := 'insert_e1_recs';
     insert_e1_recs;
     --
     v_proc_step_msg                             := 'update_t1_recs';
     update_t1_recs;
     --
  end loop;
  --
  close c1_cur;
  c1_rec.delete;   

end proc_c1_cur;


-------------------------------------------------------------------------------------------------------------
procedure validate_c2_rec
-------------------------------------------------------------------------------------------------------------
is
begin -- thgsnf_toujeo_survey_trn

  error_sw                                       := 'N';
  e1_hib_id                                      :=  c2_rec(c2).hib_id;
  e1_hib_id_seq                                  :=  c2_rec(c2).hib_id_seq;
  e1_source_table_id                             :=  t2_table_id;
  e1_target_table                                :=  t2_table_id;
  ------------------------------------------------------------------------------
  db_column_name                                 := 'hib_id';
  --
  if c2_rec(c2).m_hib_id is null
  then
     db_error_desc                               := 'not on '||t1_table_name; 
     load_e1_rec;
  end if;
  ------------------------------------------------------------------------------
  db_column_name                                 := 'question/answer';
  a1_key                                         := c2_rec(c2).question_id||'x'||c2_rec(c2).answer_id;
  if a1_rec.exists(a1_key) 
  then 
     db_error_desc                               := 'not on survey tables'; 
     load_e1_rec;
  end if;
  ------------------------------------------------------------------------------

end validate_c2_rec;


--------------------------------------------------------------------------------------------------------
PROCEDURE proc_c2_rec
--------------------------------------------------------------------------------------------------------
is
begin 

  ud_rec                                         :=  null;
  ud_rec.client_id                               :=  c2_rec(c2).client_id;
  ud_rec.table_id                                :=  c2_rec(c2).table_id;
  ud_rec.record_type                             :=  c2_rec(c2).record_type;
  ud_rec.hib_id                                  :=  c2_rec(c2).hib_id;
  ud_rec.hib_id_seq                              :=  c2_rec(c2).hib_id_seq;
  ud_rec.status                                  :=  c2_rec(c2).status;
  if error_sw = 'Y'
  then
     ud_rec.status                               := 'ERR';
  else
     ud_rec.status                               := 'PR';
  end if;

  v_proc_step_msg                                := 'load_t2_rec';
  load_t2_rec;

end proc_c2_rec;


--------------------------------------------------------------------------------------------------------
PROCEDURE proc_c2_cur
--------------------------------------------------------------------------------------------------------
is
begin 

  c2_row_cnt                                     :=  0;
  open c2_cur;
  loop 
     -- bulk collect records from cursor.   
     fetch c2_cur
     bulk collect 
     into c2_rec limit 1000;
     exit when c2_row_cnt                         =  c2_cur%rowcount;
     c2_row_cnt                                  :=  c2_cur%rowcount;
     --
     for c2_idx in 1..c2_rec.count
     loop
        c2                                       :=  c2_idx;
        c2_recs                                  :=  c2_recs + 1;
        --
        v_proc_step_msg                          := 'validate_c2_rec';
        validate_c2_rec;
        --
        v_proc_step_msg                          := 'proc_c2_rec';
        proc_c2_rec;
        --
     end loop;
     --
     v_proc_step_msg                             := 'insert_e1_recs';
     insert_e1_recs;
     --
     v_proc_step_msg                             := 'update_t2_recs';
     update_t2_recs;
     --
  end loop;
  --
  close c2_cur;
  c2_rec.delete;   

end proc_c2_cur;


-------------------------------------------------------------------------------------------------------------
procedure validate_c3_rec
-------------------------------------------------------------------------------------------------------------
is
begin -- thgsnf_toujeo_survey_trn

  error_sw                                       := 'N';
  e1_hib_id                                      :=  c3_rec(c3).hib_id;
  e1_hib_id_seq                                  :=  c3_rec(c3).hib_id_seq;
  e1_source_table_id                             :=  c3_rec(c3).table_id;
  e1_target_table                                :=  c3_rec(c3).table_id;
  ------------------------------------------------------------------------------
  db_column_name                                 := 'hib_id group error';
  db_error_desc                                  := 'parent/child errors found'; 
  load_e1_rec;
  ------------------------------------------------------------------------------

end validate_c3_rec;


--------------------------------------------------------------------------------------------------------
PROCEDURE proc_c3_rec
--------------------------------------------------------------------------------------------------------
is
begin 

  ud_rec                                         :=  null;
  ud_rec.client_id                               :=  c3_rec(c3).client_id;
  ud_rec.table_id                                :=  c3_rec(c3).table_id;
  ud_rec.record_type                             :=  c3_rec(c3).record_type;
  ud_rec.hib_id                                  :=  c3_rec(c3).hib_id;
  ud_rec.hib_id_seq                              :=  c3_rec(c3).hib_id_seq;
  ud_rec.status                                  := 'ERR';

  if c3_rec(c3).ip_record_type = '01'
  then
     v_proc_step_msg                             := 'load_t1_rec';
     load_t1_rec;
  elsif c3_rec(c3).ip_record_type = '02'
  then
     v_proc_step_msg                             := 'load_t2_rec';
     load_t2_rec;
  else
     raise_application_error(-20011,'invalid ip_record_type (01,02): '||c3_rec(c3).ip_record_type);
  end if;

end proc_c3_rec;


--------------------------------------------------------------------------------------------------------
PROCEDURE proc_c3_cur
--------------------------------------------------------------------------------------------------------
is
begin 

  c3_row_cnt                                     :=  0;
  open c3_cur;
  loop 
     -- bulk collect records from cursor.   
     fetch c3_cur
     bulk collect 
     into c3_rec limit 1000;
     exit when c3_row_cnt                         =  c3_cur%rowcount;
     c3_row_cnt                                  :=  c3_cur%rowcount;
     --
     for c3_idx in 1..c3_rec.count
     loop
        c3                                       :=  c3_idx;
        c3_recs                                  :=  c3_recs + 1;
        --
        v_proc_step_msg                          := 'validate_c3_rec';
        validate_c3_rec;
        --
        v_proc_step_msg                          := 'proc_c3_rec';
        proc_c3_rec;
        --
     end loop;
     --
     v_proc_step_msg                             := 'insert_e1_recs';
     insert_e1_recs;
     --
     v_proc_step_msg                             := 'update_t1_recs';
     update_t1_recs;
     --
     v_proc_step_msg                             := 'update_t2_recs';
     update_t2_recs;
     --
  end loop;
  --
  close c3_cur;
  c3_rec.delete;   

end proc_c3_cur;


---------------------------------------------------------------------------------------------------------------
procedure log_process_cnts
---------------------------------------------------------------------------------------------------------------
is
begin  --thgmdb_log_process_counts

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

  if t1_recs > 0
  then
     db_log_table_id                             :=  t1_table_id;
     db_log_desc                                 :=  upper('updates');
     db_log_cnt                                  :=  t1_recs;
     log_process_cnts;
  end if;
  --
  if t2_recs > 0
  then
     db_log_table_id                             :=  t2_table_id;
     db_log_desc                                 :=  upper('updates');
     db_log_cnt                                  :=  t2_recs;
     log_process_cnts;
  end if;
  --
  if e1_recs > 0
  then
     db_log_table_id                             :=  e1_table_id;
     db_log_desc                                 :=  upper('inserts');
     db_log_cnt                                  :=  e1_recs;
     log_process_cnts;
  end if;
  --
  mdb_global_pkg.activity_log_update(db_log_id, 0, 'JC',null);
  -- 
end close_process;


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
  v_proc_step_msg                                := 'proc_c1_cur';
  proc_c1_cur;
  --
  v_proc_step_msg                                := 'proc_c2_cur';
  proc_c2_cur;
  --
  v_proc_step_msg                                := 'proc_c3_cur';
  proc_c3_cur;
  --
  v_proc_step_msg                                := 'close_process';
  close_process;
  --
  v_proc_step_msg                                := 'SNF_TOUJEO_TEMPMDB_LD_SP';
  SNF_TOUJEO_TEMPMDB_LD_SP (p_vendor_id, p_email_notify);


-----------------------------------------------------------------------------------------
EXCEPTION
-----------------------------------------------------------------------------------------
  when others then
       v_log_comments                            := 'Error: '|| sqlerrm||', Proc Step: '||v_proc_step_msg;
       mdb_global_pkg.activity_log_update(db_log_id, 0, 'JE', v_log_comments);
       --
       v_email_subject                           :=  nvl(db_schema,'?')||'.'||db_procedure_name;
       end_date                                  :=  to_char(sysdate,'mm/dd/yyyy hh24:mi:ss');

       v_email_message                           :=  v_proc_msg 
                              ||v_tab||v_carr_rtn||  null
                              ||v_tab||v_carr_rtn|| 'Error.....: '||sqlerrm
                              ||v_tab||v_carr_rtn|| 'Proc Step.: '||v_proc_step_msg
                              ||v_tab||v_carr_rtn||  null
                              ||v_tab||v_carr_rtn|| 'Start Date: '||start_date||' End Date: '||end_date;
       --
       mail_files_sp('oracle',
                      p_email_notify,
                     'Error - '||v_email_subject,
                      v_email_message, 
                      32000);

END SNF_PRALUENT_TRN_PR_SP;

