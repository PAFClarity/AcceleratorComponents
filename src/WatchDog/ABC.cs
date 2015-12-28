using System;
using System.Collections.Generic;
using System.IO;
using System.Data;
using System.Data.SqlClient;

namespace WatchDog
{
    // This project can output the Class library as a NuGet Package.
    // To enable this option, right-click on the project and select the Properties menu item. In the Build tab select "Produce outputs on build".
    public class ABC:IDisposable
    {
        // This is the sole class property if properties are added
        // it would be appropriate to create get and set methods
        public string connectString;
      
        public ABC()
        {
            
            string fContent = "", cItem = "", cValue = "",tString="", path= @"C:/WatchDog/Configuration/ABCConfig.txt";
            Dictionary<string, string> configuration = new Dictionary<string, string>();

            try
            {
                using (StreamReader efile = File.OpenText(path))
                {
                    fContent = efile.ReadToEnd();
                }
                string[] lines = fContent.Split(new string[] { "\r\n", "\r", "\n" }, StringSplitOptions.RemoveEmptyEntries);
                for (int i = 0; i < lines.GetLength(0); i++)
                {
                    string[] split_words = lines[i].Split(new char[] { '|' });
                    cItem = split_words[0].ToString().Trim();
                    cValue = split_words[1].ToString().Trim();
                    configuration.Add(cItem, cValue);
                }
//The following format for extracting the connectoin string from the configuration file
// can easily be extended to populate additional class properties 
                configuration.TryGetValue("ConnectionString", out tString);
                if (tString.Length > 0) { connectString = tString.ToString().Trim(); } else { connectString = "EMPTY"; }
            }
            catch (Exception ex)
            {
                return;
            }

        }
// Notes:
// Use of try and catch around the ProcessStatus check that frames the method processing body
// and throw a user defined exception or pass predefined process status codes to caller
//
// Generate GUIDS for primary keys rather than relying on server functionality to do so.
// For non SQL Server environments GUID's can be cast to strings or to varbinary for each
// of the tables primary keys.
//
        //APPLICATION
        //This table stores the information about the Application
        public int DefineApplication(string APP_NAME, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'Y';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (APP_NAME.Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus++;
                }
            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    string insertTask = "INSERT INTO APPLICATION (APP_NAME, ENABLE_IND,UPDATE_DTTM,UPDATE_UID) VALUES (@APP_NAME, @ENABLE_IND, @UPDATE_DTTM, @UPDATE_UID)";
                    System.Data.SqlClient.SqlCommand insertTaskCmd = new System.Data.SqlClient.SqlCommand(insertTask, con);
                    insertTaskCmd.Parameters.Add("@APP_NAME", SqlDbType.VarChar).Value = APP_NAME;
                    insertTaskCmd.Parameters.Add("@ENABLE_IND", SqlDbType.VarChar).Value = ENABLE_IND;
                    insertTaskCmd.Parameters.Add("@UPDATE_DTTM", SqlDbType.DateTime2).Value = UPDATE_DTTM;
                    insertTaskCmd.Parameters.Add("@UPDATE_UID", SqlDbType.VarChar).Value = UPDATE_UID;
                    insertTaskCmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public Tuple<string, string, string, DateTime, string, int> GetApplication(string APP_ID, string APP_NAME)
            {
            //APP_ID, APP_NAME, ENABLE_IND, UPDATE_DTTM, UPDATE_UID, ProcessStatus
            int ProcessStatus = 0;
            int WhereProcess = 0;
            string WHERE_APP_ID = "'", WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if (APP_ID != null)
                {
                WhereProcess = 1;
                }
            if (APP_NAME.Trim().Length == 0)
                {
                 WhereProcess++;
                }
            if (WhereProcess == 2)
                {
                ProcessStatus++;
            }
            if ((ProcessStatus == 0) && (WhereProcess<2))
                {
                //Build Where Clause
                if (APP_ID != null)
                    {
                    WHERE_APP_ID = " APP_ID ='" + APP_ID.ToString()+"'";
                    }

                if ((APP_ID != null) && (APP_NAME.Trim().Length > 0))
                    {
                    WHERE_CLAUSE = WHERE_APP_ID + "AND  APP_NAME ='" + APP_NAME.ToString()+"'";
                    }
                else
                    {
                    WHERE_CLAUSE = " APP_NAME ='" + APP_NAME.ToString()+"'";
                    }

                //Build Query
                string queryCMD = "SELECT APP_ID, APP_NAME, ENABLE_IND, UPDATE_DTTM, UPDATE_UID FROM APPLICATION WHERE " + WHERE_CLAUSE.ToString();

                using (SqlConnection con = new SqlConnection(connectString))
                    {
                    using (SqlDataAdapter resultSet = new SqlDataAdapter(queryCMD, con))
                        {
                        resultSet.Fill(dtResult);
                        }
                    }

                //Build Return Tuple
                if (dtResult.Rows.Count > 0)
                    {
                    DataRow rstRow = dtResult.Rows[0];
                    string gValue = rstRow["APP_ID"].ToString();
                    var rtrnTuple = new Tuple<string, string, string, DateTime, string, int>(gValue.ToString(), rstRow["APP_NAME"].ToString(), rstRow["ENABLE_IND"].ToString(), (DateTime)rstRow["UPDATE_DTTM"], rstRow["UPDATE_UID"].ToString(), ProcessStatus);
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus++;
                    var rtrnTuple = new Tuple<string, string, string, DateTime, string, int>("NULL", "", "", DateTime.Now, "", ProcessStatus);
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus++;
                var rtrnTuple = new Tuple<string, string, string, DateTime, string, int>("EMPTY", "", "", DateTime.Now, "", ProcessStatus);
                return rtrnTuple;
                }

                }

        public Tuple<string, int> GetApplicationID(string iAPP_NAME)
            {
            //APP_ID, APP_NAME, ENABLE_IND, UPDATE_DTTM, UPDATE_UID, ProcessStatus
            int ProcessStatus = 0;
            DataTable dtResult = new DataTable();
            Tuple<string,int> rValue;

            if (iAPP_NAME.Trim().Length == 0)
                {
                ProcessStatus++;
                }
    
            if (ProcessStatus == 0)
                {
                //Build Where Clause
                string WHERE_CLAUSE = "APP_NAME='" + iAPP_NAME.Trim().ToString()+"'";

                //Build Query
                string queryCMD = "SELECT APP_ID FROM APPLICATION WHERE " + WHERE_CLAUSE.ToString();
                
                using (SqlConnection con = new SqlConnection(connectString))
                    {
                      SqlCommand cmd = new SqlCommand();
                    Object returnValue;

                    cmd.CommandText = queryCMD;
                    cmd.CommandType = CommandType.Text;
                    cmd.Connection = con;

                    con.Open();

                    returnValue = cmd.ExecuteScalar();

                   //Build Return Tuple
                    if (returnValue != null)
                        {
                        string gContainer = returnValue.ToString();
                        var rtrnTuple = new Tuple<string, int>(gContainer, ProcessStatus);
                        rValue = rtrnTuple;
                        }
                    else
                        {
                        //Build empty Tuple
                        ProcessStatus++;
                        var rtrnTuple = new Tuple<string, int>("NULL", ProcessStatus);
                        rValue = rtrnTuple;
                        }
                    }
                } else
                {
                //Build empty Tuple
                ProcessStatus++;
                var rtrnTuple = new Tuple<string, int>("ERROR", ProcessStatus);
                rValue = rtrnTuple;
                }
            return rValue;

            }

        public int EnableApplication(string APP_ID, string APP_NAME, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'Y';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (APP_ID.Trim().Length == 0)
                {
                ProcessStatus = 2;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 3;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " APP_ID='" + APP_ID.ToString()+"'";
                if (APP_NAME.Length > 0)
                    {
                    WHERE_CLAUSE = WHERE_CLAUSE + "AND  APP_NAME='" + APP_NAME.ToString()+"'";
                    }

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE APPLICATION SET ENABLE_IND='" + ENABLE_IND.ToString()+"', UPDATE_UID='" + UPDATE_UID.ToString() + "', UPDATE_DTTM='" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DisableApplication(string APP_ID, string APP_NAME, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'N';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (APP_ID.Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus++;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " APP_ID='" + APP_ID.ToString()+"'";
                if (APP_NAME.Length > 0)
                    {
                    WHERE_CLAUSE = WHERE_CLAUSE + "AND  APP_NAME='" + APP_NAME.ToString()+"'";
                    }

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE APPLICATION SET ENABLE_IND='" + ENABLE_IND.ToString()+"', UPDATE_UID='" + UPDATE_UID.ToString() + "', UPDATE_DTTM='" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int UpdateApplication(string APP_ID, string APP_NAME, string ENABLE_IND, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (APP_ID.Length == 0)
                {
                ProcessStatus++;
                }
            if (APP_NAME.Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " APP_ID='" + APP_ID.ToString()+"'";

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    string strEnable = "";
                    if (ENABLE_IND != null) { strEnable = "' , ENABLE_IND='" + ENABLE_IND.ToString(); }
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE APPLICATION SET APP_NAME='" + APP_NAME.ToString() +strEnable.ToString()+ "', UPDATE_UID='" + UPDATE_UID.ToString() + "',UPDATE_DTTM='" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DeleteApplication(string APP_ID)
            {
            int ProcessStatus = 0;
            if (APP_ID.Length ==0)
                {
                ProcessStatus++;
                }

            if (ProcessStatus == 0)
                {
                using (var dConn = new SqlConnection(connectString))
                using (var delCmd = dConn.CreateCommand())
                    {
                    dConn.Open();
                    delCmd.CommandType = System.Data.CommandType.Text;
                    delCmd.CommandText = "DELETE FROM APPLICATION WHERE APP_ID = @APP_ID ";
                    delCmd.Parameters.AddWithValue("@APP_ID", APP_ID.ToString());
                    delCmd.Connection = dConn;
                    delCmd.ExecuteNonQuery();
                    }

                }
            return ProcessStatus;
            }

        //BATCH
        //This table stores the information about the Batch
        public int DefineBatch(string BATCH_TYPE, string BATCH_NM, string APP_ID,string UPDATE_UID)
            {
            int ProcessStatus = 0;
            string ENABLE_IND = "Y";

            DateTime UPDATE_DTTM = DateTime.Now;

            if (BATCH_TYPE.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (APP_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (BATCH_NM.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    SqlCommand cmd = new SqlCommand("INSERT INTO BATCH (BATCH_TYPE, BATCH_NM,ENABLE_IND,APP_ID,UPDATE_DTTM,UPDATE_UID) VALUES (@BATCH_TYPE , @BATCH_NM, @ENABLE_IND,@APP_ID, @UPDATE_DTTM, @UPDATE_UID)");
                    cmd.Parameters.Add("@BATCH_TYPE",SqlDbType.VarChar).Value=BATCH_TYPE;
                    cmd.Parameters.Add("@BATCH_NM",SqlDbType.VarChar).Value=BATCH_NM;
                    cmd.Parameters.Add("@ENABLE_IND",SqlDbType.VarChar).Value=ENABLE_IND;
                    cmd.Parameters.Add("@APP_ID",SqlDbType.UniqueIdentifier).Value= new Guid(APP_ID);
                    cmd.Parameters.Add("@UPDATE_DTTM",SqlDbType.DateTime2).Value= UPDATE_DTTM;
                    cmd.Parameters.Add("@UPDATE_UID", SqlDbType.VarChar).Value=UPDATE_UID;
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public Tuple<string,int>GetBatchID(string BATCH_NM)
            {
            //BATCH_ID, BATCH_NM, BATCH_TYPE,ENABLE_IND, UPDATE_DTTM, UPDATE_UID, ProcessStatus
            int ProcessStatus = 0;
            string  WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if (BATCH_NM.Trim().Length == 0)
                {ProcessStatus++;
                }
            if (ProcessStatus == 0)
                {
                //Build Where Clause

                WHERE_CLAUSE = " BATCH_NM ='" + BATCH_NM.ToString() +"'";

                //Build Query
                string queryCMD = "SELECT BATCH_ID FROM BATCH WHERE " + WHERE_CLAUSE.ToString();

                using (SqlConnection con = new SqlConnection(connectString))
                    {
                    using (SqlDataAdapter resultSet = new SqlDataAdapter(queryCMD, con))
                        {
                        resultSet.Fill(dtResult);
                        }
                    }

                //Build Return Tuple
                if (dtResult.Rows.Count > 0)
                    {
                    DataRow rstRow = dtResult.Rows[0];
                    string rtnBATCH_ID = rstRow["BATCH_ID"].ToString();
                    var rtrnTuple = new Tuple< string, int>(rtnBATCH_ID.ToString(), ProcessStatus);
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus++;
                    var rtrnTuple = new Tuple<string, int>("EMPTY", ProcessStatus);
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus++;
                var rtrnTuple = new Tuple< string, int>("ERROR", ProcessStatus);
                return rtrnTuple;
                }
            }

        public Tuple<string, string, string, string, DateTime, string, int> GetBatch(string BATCH_ID, string BATCH_NM)
            {
            //BATCH_ID, BATCH_NM, BATCH_TYPE,ENABLE_IND, UPDATE_DTTM, UPDATE_UID, ProcessStatus
            int ProcessStatus = 0;
            int WhereProcess = 0;
            string WHERE_BATCH_ID = "", WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if (BATCH_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            else { WhereProcess++; }
            if (BATCH_NM.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            else { WhereProcess++; }

            if ((ProcessStatus == 0) && (WhereProcess >0))
                {
                //Build Where Clause

                if (WhereProcess ==2)
                    {
                    WHERE_BATCH_ID = " BATCH_ID ='" + BATCH_ID.ToString() + "'";
                    WHERE_CLAUSE = WHERE_BATCH_ID + "AND  BATCH_NM ='" + BATCH_NM.ToString()+"'";
                    }
                else
                    {
                    if (BATCH_ID.Trim().Length > 0)
                        {
                        WHERE_CLAUSE = " BATCH_ID ='" + BATCH_ID.ToString() + "'";
                        } else { WHERE_CLAUSE = " BATCH_NM =" + BATCH_NM.ToString()+"'"; }                   
                    }

                //Build Query
                string queryCMD = "SELECT BATCH_ID, BATCH_NM, BATCH_TYPE,ENABLE_IND, UPDATE_DTTM, UPDATE_UID FROM BATCH WHERE " + WHERE_CLAUSE.ToString();

                using (SqlConnection con = new SqlConnection(connectString))
                    {
                    using (SqlDataAdapter resultSet = new SqlDataAdapter(queryCMD, con))
                        {
                        resultSet.Fill(dtResult);
                        }
                    }

                //Build Return Tuple
                if (dtResult.Rows.Count > 0)
                    {
                    DataRow rstRow = dtResult.Rows[0];
                    var rtrnTuple = new Tuple<string, string, string, string, DateTime, string, int>(rstRow["BATCH_ID"].ToString(), rstRow["BATCH_NM"].ToString(), rstRow["BATCH_TYPE"].ToString(), rstRow["ENABLE_IND"].ToString(), (DateTime)rstRow["UPDATE_DTTM"], rstRow["UPDATE_UID"].ToString(), ProcessStatus);
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus++;
                    var rtrnTuple = new Tuple<string, string, string, string, DateTime, string, int>("EMPTY", "", "", "", DateTime.Now, "", ProcessStatus);
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus++;
                var rtrnTuple = new Tuple<string, string, string, string, DateTime, string, int>("ERROR", "", "", "", DateTime.Now, "", ProcessStatus);
                return rtrnTuple;
                }

            }

        public int EnableBatch(string BATCH_ID, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'Y';
            DateTime UPDATE_DTTM = DateTime.Now;
            string WHERE_CLAUSE = "";
            WHERE_CLAUSE = " BATCH_ID='" + BATCH_ID.ToString() + "'";

            if (BATCH_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE BATCH SET ENABLE_IND='" + ENABLE_IND.ToString() + "', UPDATE_UID='" + UPDATE_UID.ToString() + "', UPDATE_DTTM='" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DisableBatch(string BATCH_ID, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'N';
            DateTime UPDATE_DTTM = DateTime.Now;
            string WHERE_CLAUSE = "";
            WHERE_CLAUSE = " BATCH_ID='" + BATCH_ID.ToString() + "'";

            if (BATCH_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (ProcessStatus == 0)
                {

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE BATCH SET ENABLE_IND='" + ENABLE_IND.ToString() + "', UPDATE_UID='" + UPDATE_UID.ToString() + "', UPDATE_DTTM='" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int UpdateBatch(string BATCH_ID, string BATCH_NM, string BATCH_TYPE, string ENABLE_IND, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (BATCH_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if ((BATCH_NM.Trim().Length == 0) && (BATCH_TYPE.Trim().Length == 0))
                {
                ProcessStatus++;
                }

            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " BATCH_ID='" + BATCH_ID.ToString()+"'";
                string SET_CLAUSE = "";
                if ((BATCH_NM.Trim().Length > 0) && (BATCH_TYPE.Trim().Length > 0))
                    {
                    SET_CLAUSE = " BATCH_NM='" + BATCH_NM.ToString() + "', BATCH_TYPE='" + BATCH_TYPE.ToString() + "' ";
                    }
                if (BATCH_TYPE.Trim().Length > 0)
                        {
                    SET_CLAUSE = "BATCH_TYPE='" + BATCH_TYPE.ToString() + "' ";
                    } else {
                    SET_CLAUSE = " BATCH_NM='" + BATCH_NM.ToString() + "' "; } 
                if (ENABLE_IND.Trim().Length>0) { SET_CLAUSE = SET_CLAUSE + ", ENABLE_IND='" + ENABLE_IND.ToString()+"'"; }

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE APPLICATION SET " + SET_CLAUSE.ToString() + ", UPDATE_UID='" + UPDATE_UID.ToString() + "', UPDATE_DTTM=" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DeleteBatch (string BATCH_ID)
            {
            int ProcessStatus = 0;


            if (BATCH_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            else
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = "WHERE BATCH_ID='" + BATCH_ID.ToString() + "'";

                using (var dConn = new SqlConnection(connectString))
               // using (var delCmd = dConn.CreateCommand())
                    {
                    dConn.Open();
                    System.Data.SqlClient.SqlCommand delCmd = new System.Data.SqlClient.SqlCommand();
                    delCmd.CommandType = System.Data.CommandType.Text;
                    delCmd.CommandText = "DELETE FROM BATCH " + WHERE_CLAUSE.ToString();
                    delCmd.Connection = dConn;
                    delCmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        //BATCH_CONTROL
        //This table stores the information about each Batch execution
        public int InvokeBatchControl(string BATCH_ID, string BATCH_CNTRL_NM, DateTime BATCH_CNTRL_DT, string BATCH_CNTRL_ACTV_IND, DateTime BATCH_STRT_DTTM, DateTime BOUNDARY_START_DT, DateTime BOUNDARY_END_DT, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (BATCH_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (BATCH_CNTRL_NM.Length == 0)
                {
                ProcessStatus++;
                }
            if (BATCH_CNTRL_ACTV_IND.ToString().Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus++;
                }
            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    string insertTask = "INSERT INTO BATCH_CONTROL (BATCH_ID, BATCH_CNTRL_NM,  BATCH_CNTRL_DT, BATCH_CNTRL_ACTV_IND, BATCH_STRT_DTTM, BOUNDARY_START_DT, BOUNDARY_END_DT, UPDATE_DTTM,UPDATE_UID) VALUES (@BATCH_ID, @BATCH_CNTRL_NM, @BATCH_CNTRL_DT, @BATCH_CNTRL_ACTV_IND, @BATCH_STRT_DTTM, @BOUNDARY_START_DT, @BOUNDARY_END_DT, @UPDATE_DTTM, @UPDATE_UID)";
                    System.Data.SqlClient.SqlCommand insertTaskCmd = new System.Data.SqlClient.SqlCommand(insertTask, con);
                    insertTaskCmd.Parameters.Add("@BATCH_ID", SqlDbType.UniqueIdentifier).Value = new Guid(BATCH_ID);
                    insertTaskCmd.Parameters.Add("@BATCH_CNTRL_NM", SqlDbType.VarChar).Value = BATCH_CNTRL_NM;
                    insertTaskCmd.Parameters.Add("@BATCH_CNTRL_DT", SqlDbType.DateTime2).Value = BATCH_CNTRL_DT;
                    insertTaskCmd.Parameters.Add("@BATCH_CNTRL_ACTV_IND", SqlDbType.VarChar).Value = BATCH_CNTRL_ACTV_IND;
                    insertTaskCmd.Parameters.Add("@BATCH_STRT_DTTM", SqlDbType.DateTime2).Value = BATCH_STRT_DTTM;
                    insertTaskCmd.Parameters.Add("@BOUNDARY_START_DT", SqlDbType.DateTime2).Value = BOUNDARY_START_DT;
                    insertTaskCmd.Parameters.Add("@BOUNDARY_END_DT", SqlDbType.DateTime2).Value = BOUNDARY_END_DT;
                    insertTaskCmd.Parameters.Add("@UPDATE_DTTM", SqlDbType.DateTime2).Value = UPDATE_DTTM;
                    insertTaskCmd.Parameters.Add("@UPDATE_UID", SqlDbType.VarChar).Value = UPDATE_UID;
                    insertTaskCmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public Tuple<string, int> GetBatchControlID(string BATCH_CNTRL_NM)
            {
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if (BATCH_CNTRL_NM.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (ProcessStatus == 0)
                {
                //Build Where Clause

                WHERE_CLAUSE = " BATCH_CNTRL_NM ='" + BATCH_CNTRL_NM.ToString() + "'";

                //Build Query
                string queryCMD = "SELECT BATCH_CNTRL_ID FROM BATCH_CONTROL WHERE " + WHERE_CLAUSE.ToString();

                using (SqlConnection con = new SqlConnection(connectString))
                    {
                    using (SqlDataAdapter resultSet = new SqlDataAdapter(queryCMD, con))
                        {
                        resultSet.Fill(dtResult);
                        }
                    }

                //Build Return Tuple
                if (dtResult.Rows.Count > 0)
                    {
                    DataRow rstRow = dtResult.Rows[0];
                    var rtrnTuple = new Tuple<string, int>(rstRow["BATCH_CNTRL_ID"].ToString(), ProcessStatus);
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus++;
                    var rtrnTuple = new Tuple<string, int>("EMPTY", ProcessStatus);
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build error Tuple
                ProcessStatus++;
                var rtrnTuple = new Tuple<string, int>("ERROR", ProcessStatus);
                return rtrnTuple;
                }

            }

        public Tuple<string, string, string, DateTime, string, string, int,Tuple<DateTime, DateTime, DateTime, DateTime, DateTime> > GetBatchControl(string BATCH_CNTRL_ID, string BATCH_CNTRL_NM)
            {
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";


            if (BATCH_CNTRL_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                } 
            if (BATCH_CNTRL_NM.Trim().Length == 0)
                {
                ProcessStatus++;
                }


            if (ProcessStatus == 0)
                {
                //Build Where Clause

                    WHERE_CLAUSE = " BATCH_CNTRL_ID ='" + new Guid(BATCH_CNTRL_ID).ToString() + "' AND  BATCH_CNTRL_NM ='" + BATCH_CNTRL_NM.ToString() + "'";

                
                //Build Query
                string queryCMD = "SELECT * FROM BATCH_CONTROL WHERE " + WHERE_CLAUSE.ToString();

                using (SqlConnection con = new SqlConnection(connectString))
                using (SqlDataAdapter nresultSet = new SqlDataAdapter(queryCMD, con))
                    {
                    DataTable ndtResult = new DataTable();
                    con.Open();
                    nresultSet.Fill(ndtResult);

                    //Build Return Tuple
                    if (ndtResult.Rows.Count > 0)
                       {
                       DataRow rstRow = ndtResult.Rows[0];
                        DateTime tpBATCH_STRT_DTTM;
                        DateTime.TryParse(rstRow["BATCH_STRT_DTTM"].ToString(), out tpBATCH_STRT_DTTM);
                        DateTime tpBATCH_END_DTTM;
                        DateTime.TryParse(rstRow["BATCH_END_DTTM"].ToString(), out tpBATCH_END_DTTM);
                        var rtrnTuple = new Tuple<string, string, string, DateTime, string, string, int, Tuple<DateTime, DateTime, DateTime, DateTime, DateTime>>(rstRow["BATCH_CNTRL_ID"].ToString(), rstRow["BATCH_CNTRL_NM"].ToString(), rstRow["BATCH_ID"].ToString(), (DateTime)rstRow["BATCH_CNTRL_DT"], rstRow["BATCH_CNTRL_ACTV_IND"].ToString(), rstRow["UPDATE_UID"].ToString(), ProcessStatus, new Tuple<DateTime, DateTime, DateTime, DateTime, DateTime>((DateTime) tpBATCH_STRT_DTTM, (DateTime)tpBATCH_END_DTTM, (DateTime)rstRow["BOUNDARY_START_DT"], (DateTime)rstRow["BOUNDARY_END_DT"], (DateTime)rstRow["UPDATE_DTTM"]));
                        return rtrnTuple;
                        }
                    else
                    {
                    //Build empty Tuple
                    ProcessStatus++;
                        var rtrnTuple = new Tuple<string, string, string, DateTime, string, string, int, Tuple<DateTime, DateTime, DateTime, DateTime, DateTime>>("EMPTY", "", "", DateTime.Now, "", "", ProcessStatus, new Tuple<DateTime, DateTime, DateTime, DateTime, DateTime>(DateTime.Now, DateTime.Now, DateTime.Now, DateTime.Now, DateTime.Now));
                        return rtrnTuple;
                      }
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus++;
                var rtrnTuple = new Tuple<string, string, string, DateTime, string, string, int, Tuple<DateTime, DateTime, DateTime, DateTime, DateTime>>("ERROR", "", "", DateTime.Now, "", "", ProcessStatus, new Tuple<DateTime, DateTime, DateTime, DateTime, DateTime>(DateTime.Now, DateTime.Now, DateTime.Now, DateTime.Now, DateTime.Now));
                return rtrnTuple;
                }

            }


        public int UpdateBatchControl(string BATCH_CNTRL_ID, string BATCH_CNTRL_NM, DateTime BATCH_CNTRL_DT, string BATCH_CNTRL_ACTV_IND, DateTime BATCH_STRT_DTTM, DateTime BATCH_END_DTTM, DateTime BOUNDARY_START_DT, DateTime BOUNDARY_END_DT, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (BATCH_CNTRL_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (ProcessStatus == 0)
                {
                //Build Where Clause
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " BATCH_CNTRL_ID='" + BATCH_CNTRL_ID.ToString()+"'";

                //Build Set Clause
                string SET_CLAUSE = "";
                string SET_COMMA = " ";
                int SetCounter = 0;

                //BATCH_CNTRL_NM
                if (BATCH_CNTRL_NM.Trim().Length > 0)
                    {
                    SET_CLAUSE = "BATCH_CNTRL_NM='" + BATCH_CNTRL_NM.ToString()+"'";
                    SetCounter++;
                    }
                //BATCH_CNTRL_DT
                if (BATCH_CNTRL_DT != null)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + "BATCH_CNTRL_DT='"+ BATCH_CNTRL_DT.ToString()+"'";			   
                }
                //BATCH_CNTRL_ACTV_IND
                if (BATCH_CNTRL_ACTV_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + "BATCH_CNTRL_ACTV_IND='"+ BATCH_CNTRL_ACTV_IND.ToString()+"'";			   
                }
                //BATCH_START_DT
                if (BATCH_STRT_DTTM != null)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + "BATCH_STRT_DTTM='" + BATCH_STRT_DTTM.ToString()+"'";			   
                }
                //BATCH_END_DT
                if (BATCH_END_DTTM != null)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + "BATCH_END_DTTM='" + BATCH_END_DTTM.ToString()+"'";			   
                }
                //BOUNDARY_START_DT
                if (BOUNDARY_START_DT != null)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + "BOUNDARY_START_DT='"+ BOUNDARY_START_DT.ToString()+"'";			   
                }
                //BOUNDARY_END_DT
                if (BOUNDARY_END_DT != null)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + "BOUNDARY_END_DT='"+BOUNDARY_END_DT.ToString()+"'";			   
                }
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE BATCH_CONTROL SET " + SET_CLAUSE.ToString() + ", UPDATE_UID='" + UPDATE_UID.ToString() + "', UPDATE_DTTM='" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        //PROCESS
        //This table stores the information about a Process
        public int DefineProcess(string PROCESS_ID_NM, string BATCH_ID, string PROCESS_TYP_CD, string PROCESS_LOCATION, string PROCESS_NM, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROCESS_ID_NM.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (BATCH_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (PROCESS_TYP_CD.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (PROCESS_LOCATION.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (PROCESS_NM.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    SqlCommand cmd = new SqlCommand("INSERT INTO PROCESS (PROCESS_ID_NM, BATCH_ID, PROCESS_TYP_CD,  PROCESS_LOCATION, PROCESS_NM, ENABLE_IND, UPDATE_DTTM,UPDATE_UID) VALUES (@PROCESS_ID_NM, @BATCH_ID, @PROCESS_TYP_CD, @PROCESS_LOCATION, @PROCESS_NM, @ENABLE_IND, @UPDATE_DTTM, @UPDATE_UID)");
                    cmd.Parameters.Add("@PROCESS_ID_NM", SqlDbType.VarChar).Value = PROCESS_ID_NM.Trim().ToString();
                    cmd.Parameters.Add("@BATCH_ID", SqlDbType.UniqueIdentifier).Value = new Guid(BATCH_ID);
                     cmd.Parameters.Add("@PROCESS_TYP_CD", SqlDbType.VarChar,15).Value = PROCESS_TYP_CD.Trim().ToString();
                    cmd.Parameters.Add("@PROCESS_LOCATION", SqlDbType.VarChar).Value = PROCESS_LOCATION.Trim().ToString();
                    cmd.Parameters.Add("@PROCESS_NM", SqlDbType.VarChar).Value = PROCESS_NM.Trim().ToString();
                    cmd.Parameters.Add("@ENABLE_IND", SqlDbType.VarChar).Value = "Y";
                    cmd.Parameters.Add("@UPDATE_DTTM", SqlDbType.DateTime2).Value = UPDATE_DTTM;
                    cmd.Parameters.Add("@UPDATE_UID", SqlDbType.VarChar).Value = UPDATE_UID.Trim().ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public Tuple<string, int> GetProcessID(string PROCESS_ID_NM)
            {
            //Tuple<PROCESS_ID,PROCESS_ID_NM,BATCH_ID,ProcessStatus,Tuple< PROCESS_TYP_CD, PROCESS_LOCATION, PROCESS_NM, ENABLE_IND, UPDATE_DTTM, UPDATE_UID>>
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if (PROCESS_ID_NM.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if ((ProcessStatus == 0))
                {
                //Build Where Clause

                WHERE_CLAUSE = " PROCESS_ID_NM ='" + PROCESS_ID_NM.ToString() + "'";


                //Build Query
                string queryCMD = "SELECT PROCESS_ID FROM PROCESS WHERE " + WHERE_CLAUSE.ToString();

                using (SqlConnection con = new SqlConnection(connectString))
                    {
                    using (SqlDataAdapter resultSet = new SqlDataAdapter(queryCMD, con))
                        {
                        resultSet.Fill(dtResult);
                        }
                    }

                //Build Return Tuple
                //Tuple<PROCESS_ID, ProcessStatus>

                if (dtResult.Rows.Count > 0)
                    {
                    DataRow rstRow = dtResult.Rows[0];
                    var rtrnTuple = new Tuple<string, int>(rstRow["PROCESS_ID"].ToString(), ProcessStatus);
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus++;
                    var rtrnTuple = new Tuple<string, int>("EMPTY", ProcessStatus);
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build error Tuple
                ProcessStatus++;
                var rtrnTuple = new Tuple<string, int>("ERROR", ProcessStatus);
                return rtrnTuple;
                }

            }

        public Tuple<string, string, string, int, Tuple<string, string, string, string, DateTime, string>> GetProcess(string PROCESS_ID)
            {
            //Tuple<PROCESS_ID,PROCESS_ID_NM,BATCH_ID,ProcessStatus,Tuple< PROCESS_TYP_CD, PROCESS_LOCATION, PROCESS_NM, ENABLE_IND, UPDATE_DTTM, UPDATE_UID>>
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if (PROCESS_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if ((ProcessStatus == 0))
                {
                //Build Where Clause

                WHERE_CLAUSE = " PROCESS_ID ='" + PROCESS_ID.ToString()+"'";


                //Build Query
                string queryCMD = "SELECT PROCESS_ID,PROCESS_ID_NM,BATCH_ID,PROCESS_TYP_CD, PROCESS_LOCATION, PROCESS_NM, ENABLE_IND, UPDATE_DTTM, UPDATE_UID FROM PROCESS WHERE " + WHERE_CLAUSE.ToString();

                using (SqlConnection con = new SqlConnection(connectString))
                    {
                    using (SqlDataAdapter resultSet = new SqlDataAdapter(queryCMD, con))
                        {
                        resultSet.Fill(dtResult);
                        }
                    }

                //Build Return Tuple
                //Tuple<PROCESS_ID, PROCESS_ID_NM,BATCH_ID,Tuple< PROCESS_TYP_CD, PROCESS_LOCATION, PROCESS_NM, ENABLE_IND, UPDATE_DTTM, UPDATE_UID>, ProcessStatus>

                if (dtResult.Rows.Count > 0)
                    {
                    DataRow rstRow = dtResult.Rows[0];
                    var rtrnTuple = new Tuple<string, string, string, int, Tuple<string, string, string, string, DateTime, string>>(rstRow["PROCESS_ID"].ToString(), rstRow["PROCESS_ID_NM"].ToString(), rstRow["BATCH_ID"].ToString(), ProcessStatus, new Tuple<string, string, string, string, DateTime,string>(rstRow["PROCESS_TYP_CD"].ToString(), rstRow["PROCESS_LOCATION"].ToString(), rstRow["PROCESS_NM"].ToString(), rstRow["ENABLE_IND"].ToString(), (DateTime)rstRow["UPDATE_DTTM"], rstRow["UPDATE_UID"].ToString()));
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus++;
                    var rtrnTuple = new Tuple<string, string, string, int, Tuple<string, string, string, string, DateTime, string>>("EMPTY", "", "", ProcessStatus, new Tuple<string, string, string, string, DateTime, string>("", "", "", "", DateTime.Now, ""));
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus++;
                var rtrnTuple = new Tuple<string, string, string, int, Tuple<string, string, string, string, DateTime, string>>("ERROR", "", "", ProcessStatus, new Tuple<string, string, string, string, DateTime, string>("", "", "", "", DateTime.Now, ""));
                return rtrnTuple;
                }

            }

        public int EnableProcess(string PROCESS_ID, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'Y';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROCESS_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus++;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " PROCESS_ID='" + PROCESS_ID.ToString()+"'";

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE PROCESS SET ENABLE_IND='" + ENABLE_IND.ToString() + "', UPDATE_UID='" + UPDATE_UID.ToString() + "', UPDATE_DTTM='" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DisableProcess(string PROCESS_ID, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'N';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROCESS_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " PROCESS_ID='" + PROCESS_ID.ToString()+"'";


                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE PROCESS SET ENABLE_IND='" + ENABLE_IND.ToString() + "', UPDATE_UID='" + UPDATE_UID.ToString() + "', UPDATE_DTTM='" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int UpdateProcess(string PROCESS_ID, string PROCESS_ID_NM, string BATCH_ID, string PROCESS_TYP_CD, string PROCESS_LOCATION, string PROCESS_NM, string ENABLE_IND, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROCESS_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (ProcessStatus == 0)
                {
                //Build Where Clause
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " PROCESS_ID='" + PROCESS_ID.ToString()+"'";

                //Build Set Clause
                string SET_CLAUSE = "";
                string SET_COMMA = "";
                int SetCounter = 0;

                //PROCESS_ID_NM
                if (PROCESS_ID_NM.Trim().Length > 0)
                    {
                    SET_CLAUSE = "PROCESS_ID_NM='" + PROCESS_ID_NM.ToString()+"'";
                    SetCounter++;
                    }
                //BATCH_ID
                if (BATCH_ID.Trim().Length == 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE + "BATCH_ID='" + BATCH_ID.ToString()+"'";
                    SetCounter++;
                    }
                //PROCESS_TYP_CD
                if (PROCESS_TYP_CD.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + "PROCESS_TYP_CD='"+PROCESS_TYP_CD.ToString()+"'";			   
                }
                //PROCESS_LOCATION
                if (PROCESS_LOCATION.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + "PROCESS_LOCATION='"+PROCESS_LOCATION.ToString()+"'";			   
                }
                //PROCESS_NM
                if (PROCESS_NM.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + "PROCESS_NM='"+ PROCESS_NM.ToString()+"'";			   
                }
                //ENABLE_IND
                if (ENABLE_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + "ENABLE_IND='"+ENABLE_IND.ToString()+"'";			   
                }

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    SqlCommand cmd = new SqlCommand("UPDATE PROCESS SET " + SET_CLAUSE.ToString() + ", UPDATE_UID='" + UPDATE_UID.ToString() + "', UPDATE_DTTM='" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString());
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DeleteProcess(string PROCESS_ID)
            {
            int ProcessStatus = 0;
            if (PROCESS_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            else
                {
                using (var dConn = new SqlConnection(connectString))
                using (var delCmd = dConn.CreateCommand())
                    {
                    dConn.Open();
                    delCmd.CommandText = "DELETE FROM PROCESS WHERE PROCESS_ID = @PROCESS_ID";
                    delCmd.Parameters.AddWithValue("@PROCESS_ID", PROCESS_ID.ToString());
                    delCmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        //AUDIT_BALANCE_DEFINITION
        //This table stores the formulas used for performing Audit, Balance and Control
        public int InsertAuditBalanceDefinition(string PROCESS_ID, string SOURCE_EXPRESSION_TXT, string OPERAND, string TARGET_EXPRESSION_TXT, string ABC_TYP_IND, string ABC_FAIL_CRITICAL_FLAG, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROCESS_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (SOURCE_EXPRESSION_TXT.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (OPERAND.Length == 0)
                {
                ProcessStatus++;
                }
            if (TARGET_EXPRESSION_TXT.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (ABC_TYP_IND.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (ABC_FAIL_CRITICAL_FLAG.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus++;
                }
            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    Guid ngABCRULEID = Guid.NewGuid();
                    con.Open();
                    string insertTask = "INSERT INTO AUDIT_BALANCE_DEFINITION (PROCESS_ID, ABC_RULE_ID, SOURCE_EXPRESSION_TXT, OPERAND,TARGET_EXPRESSION_TXT, ABC_TYP_IND, ENABLE_IND, ABC_FAIL_CRITICAL_FLAG,UPDATE_DTTM,UPDATE_UID) VALUES (@PROCESS_ID, @ABC_RULE_ID, @SOURCE_EXPRESSION_TXT, @OPERAND, @TARGET_EXPRESSION_TXT, @ABC_TYP_IND, @ENABLE_IND, @ABC_FAIL_CRITICAL_FLAG, @UPDATE_DTTM, @UPDATE_UID)";
                    System.Data.SqlClient.SqlCommand insertTaskCmd = new SqlCommand(insertTask, con);
                    insertTaskCmd.Parameters.Add("@PROCESS_ID", SqlDbType.UniqueIdentifier).Value = new Guid(PROCESS_ID);
                    insertTaskCmd.Parameters.Add("@ABC_RULE_ID", SqlDbType.UniqueIdentifier).Value = ngABCRULEID;
                    insertTaskCmd.Parameters.Add("@SOURCE_EXPRESSION_TXT", SqlDbType.VarChar).Value = SOURCE_EXPRESSION_TXT.Trim().ToString();
                    insertTaskCmd.Parameters.Add("@OPERAND", SqlDbType.VarChar).Value = OPERAND.Trim().ToString();
                    insertTaskCmd.Parameters.Add("@TARGET_EXPRESSION_TXT", SqlDbType.VarChar).Value = TARGET_EXPRESSION_TXT.Trim().ToString();
                    insertTaskCmd.Parameters.Add("@ABC_TYP_IND", SqlDbType.VarChar).Value = ABC_TYP_IND.Trim().ToString();
                    insertTaskCmd.Parameters.Add("@ENABLE_IND", SqlDbType.VarChar).Value = "Y";
                    insertTaskCmd.Parameters.Add("@ABC_FAIL_CRITICAL_FLAG", SqlDbType.VarChar).Value = ABC_FAIL_CRITICAL_FLAG.Trim().ToString();
                    insertTaskCmd.Parameters.Add("@UPDATE_DTTM", SqlDbType.DateTime2).Value = UPDATE_DTTM;
                    insertTaskCmd.Parameters.Add("@UPDATE_UID", SqlDbType.VarChar).Value = UPDATE_UID.Trim().ToString();
                    insertTaskCmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public Tuple<string, int> GetABCRuleID(string PROCESS_ID,string SOURCE_EXPRESSION_TXT)
            {
            //PROCESS_ID, ABC_RULE_ID
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if (PROCESS_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (SOURCE_EXPRESSION_TXT.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if ((ProcessStatus == 0))
                {
                //Build Where Clause

                WHERE_CLAUSE = " PROCESS_ID ='" + PROCESS_ID.ToString() + "' AND SOURCE_EXPRESSION_TXT='" + SOURCE_EXPRESSION_TXT.ToString()+"'";


                //Build Query
                string queryCMD = "SELECT ABC_RULE_ID FROM AUDIT_BALANCE_DEFINITION WHERE " + WHERE_CLAUSE.ToString();

                using (SqlConnection con = new SqlConnection(connectString))
                    {
                    using (SqlDataAdapter resultSet = new SqlDataAdapter(queryCMD, con))
                        {
                        resultSet.Fill(dtResult);
                        }
                    }

                //Build Return Tuple
                //Tuple<ABC_RULE_ID,ProcessStatus>

                if (dtResult.Rows.Count > 0)
                    {
                    DataRow rstRow = dtResult.Rows[0];
                    var rtrnTuple = new Tuple<string, int>(rstRow["ABC_RULE_ID"].ToString(), ProcessStatus);
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus++;
                    var rtrnTuple = new Tuple<string, int>("EMPTY", ProcessStatus);
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build error Tuple
                ProcessStatus++;
                var rtrnTuple = new Tuple<string, int>("ERROR", ProcessStatus);
                return rtrnTuple;
                }

            }

        public Tuple<string, string, DateTime, string, int, Tuple<string, string, string, string, string, string>> GetAuditBalanceDefinition(string PROCESS_ID, string ABC_RULE_ID)
            {
            //PROCESS_ID, ABC_RULE_ID, UPDATE_DTTM, UPDATE_UID, PROCESS_STATUS, inner tuple(SOURCE_EXPRESSION_TXT, OPERAND, TARGET_EXPRESSION_TXT, ABC_TYP_IND, ENABLE_IND, ABC_FAIL_CRITICAL_FLAG)
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if ((PROCESS_ID.Trim().Length == 0) && (ABC_RULE_ID.Trim().Length == 0))
                {
                ProcessStatus++;
                }

            if ((ProcessStatus == 0))
                {
                //Build Where Clause

                WHERE_CLAUSE = " PROCESS_ID ='" + PROCESS_ID.ToString() + "' AND ABC_RULE_ID ='" + ABC_RULE_ID.ToString()+"'";


                //Build Query
                string queryCMD = "SELECT PROCESS_ID, ABC_RULE_ID, UPDATE_DTTM, UPDATE_UID, SOURCE_EXPRESSION_TXT, OPERAND, TARGET_EXPRESSION_TXT, ABC_TYP_IND, ENABLE_IND, ABC_FAIL_CRITICAL_FLAG FROM AUDIT_BALANCE_DEFINITION WHERE " + WHERE_CLAUSE.ToString();

                using (SqlConnection con = new SqlConnection(connectString))
                    {
                    using (SqlDataAdapter resultSet = new SqlDataAdapter(queryCMD, con))
                        {
                        resultSet.Fill(dtResult);
                        }
                    }

                //Build Return Tuple
                //Tuple<PROCESS_ID, ABC_RULE_ID, UPDATE_DTTM, UPDATE_UID, Tuple> SOURCE_EXPRESSION_TXT, OPERAND, TARGET_EXPRESSION_TXT, ABC_TYP_IND, ENABLE_IND, ABC_FAIL_CRITICAL_FLAG>>

                if (dtResult.Rows.Count > 0)
                    {
                    DataRow rstRow = dtResult.Rows[0];
                    var rtrnTuple = new Tuple<string, string, DateTime, string, int, Tuple<string, string, string, string, string, string>>(rstRow["PROCESS_ID"].ToString(), rstRow["ABC_RULE_ID"].ToString(), (DateTime)rstRow["UPDATE_DTTM"], rstRow["UPDATE_UID"].ToString(), ProcessStatus, new Tuple<string, string, string, string, string, string>(rstRow["SOURCE_EXPRESSION_TXT"].ToString(), rstRow["OPERAND"].ToString(), rstRow["TARGET_EXPRESSION_TXT"].ToString(), rstRow["ABC_TYP_IND"].ToString(), rstRow["ENABLE_IND"].ToString(), rstRow["ABC_FAIL_CRITICAL_FLAG"].ToString()));
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus++;
                    var rtrnTuple = new Tuple<string, string, DateTime, string, int, Tuple<string, string, string, string, string, string>>("EMPTY", "", DateTime.Now, "", ProcessStatus, new Tuple<string, string, string, string, string, string>("", "", "", "", "", ""));
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus++;
                var rtrnTuple = new Tuple<string, string, DateTime, string, int, Tuple<string, string, string, string, string, string>>("ERROR", "", DateTime.Now, "", ProcessStatus, new Tuple<string, string, string, string, string, string>("", "", "", "", "", ""));
                return rtrnTuple;
                }

            }

        public int EnableAuditBalanceDefinition(string ABC_RULE_ID, string PROCESS_ID, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'Y';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (ABC_RULE_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (PROCESS_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " ABC_RULE_ID='" + ABC_RULE_ID.ToString() + "' AND PROCESS_ID='"+ PROCESS_ID.ToString()+"'";

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE AUDIT_BALANCE_DEFINITION SET ENABLE_IND='" + ENABLE_IND.ToString() + "', UPDATE_UID='" + UPDATE_UID.ToString() + "', UPDATE_DTTM='" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DisableAuditBalanceDefinition(string ABC_RULE_ID, string PROCESS_ID, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'N';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (ABC_RULE_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (PROCESS_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " ABC_RULE_ID='" + ABC_RULE_ID.ToString() + "' AND PROCESS_ID='"+ PROCESS_ID.ToString()+"'";


                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE AUDIT_BALANCE_DEFINITION SET ENABLE_IND='" + ENABLE_IND.ToString() + "', UPDATE_UID='" + UPDATE_UID.ToString() + "', UPDATE_DTTM='" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int UpdateAuditBalanceDefinition(string ABC_RULE_ID, string PROCESS_ID, string SOURCE_EXPRESSION_TXT, string OPERAND, string TARGET_EXPRESSION_TXT, string ABC_TYP_IND, string ENABLE_IND, string ABC_FAIL_CRITICAL_FLAG, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (ABC_RULE_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (PROCESS_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (ProcessStatus == 0)
                {
                //Build Where Clause
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = "ABC_RULE_ID='" + ABC_RULE_ID.ToString() + "' AND PROCESS_ID='" + PROCESS_ID.ToString()+"'";

                //Build Set Clause
                string SET_CLAUSE = "";
                string SET_COMMA = "";
                int SetCounter = 0;

                //SOURCE_EXPRESSION_TXT
                if (SOURCE_EXPRESSION_TXT.Trim().Length > 0)
                    {
                    SET_CLAUSE = "SOURCE_EXPRESSION_TXT='" + SOURCE_EXPRESSION_TXT.ToString()+"'";
                    SetCounter++;
                    }
                //OPERAND
                if (OPERAND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + "OPERAND='"+OPERAND.ToString()+"'";
                    }
                //TARGET_EXPRESSION_TXT
                if (TARGET_EXPRESSION_TXT.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + "TARGET_EXPRESSION_TXT='"+TARGET_EXPRESSION_TXT.ToString()+"'";
                    }
                //ABC_TYP_IND
                if (ABC_TYP_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + "ABC_TYP_IND='"+ ABC_TYP_IND.ToString()+"'";
                    }
                //ENABLE_IND
                if (ENABLE_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + "ENABLE_IND='"+ENABLE_IND.ToString()+"'";
                    }
                //ABC_FAIL_CRITICAL_FLAG
                if (ABC_FAIL_CRITICAL_FLAG.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + "ABC_FAIL_CRITICAL_FLAG='"+ ABC_FAIL_CRITICAL_FLAG.ToString()+"'";
                    }

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE AUDIT_BALANCE_DEFINITION SET " + SET_CLAUSE.ToString() + ", UPDATE_UID='" + UPDATE_UID.ToString() + "', UPDATE_DTTM='" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DeleteAuditBalanceDefinition(string PROCESS_ID, string ABC_RULE_ID)
            {
            int ProcessStatus = 0;
            if ((PROCESS_ID.Trim().Length == 0) || (ABC_RULE_ID.Trim().Length == 0))
                {
                ProcessStatus++;
                }
            else
                {
                using (var dConn = new SqlConnection(connectString))
                using (var delCmd = dConn.CreateCommand())
                    {
                    dConn.Open();
                    delCmd.CommandText = "DELETE FROM AUDIT_BALANCE_DEFINITION WHERE PROCESS_ID = @PROCESS_ID AND ABC_RULE_ID=@ABC_RULE_ID";
                    delCmd.Parameters.Add("@PROCESS_ID", SqlDbType.UniqueIdentifier).Value = new Guid(PROCESS_ID);
                    delCmd.Parameters.Add("@ABC_RULE_ID", SqlDbType.UniqueIdentifier).Value = new Guid(ABC_RULE_ID);
                    delCmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        //PROCESS_CONTROL
        //This table stores the information about each Process execution
        public int DefineProcessControl(string PROCESS_ID, string BATCH_CNTRL_ID, string PROCESS_CNTRL_STS, DateTime PROCESS_STR_DTTM, DateTime PROCESS_INIT_STR_DTTM)
            {
            int ProcessStatus = 0;
            Int64 PROCESS_RESTR_CNTR = 0;

            if (PROCESS_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (BATCH_CNTRL_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (PROCESS_CNTRL_STS.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    string insertTask = "INSERT INTO PROCESS_CONTROL (BATCH_CNTRL_ID,PROCESS_ID, PROCESS_CNTRL_STS, PROCESS_STR_DTTM, PROCESS_RESTR_CNTR, PROCESS_INIT_STR_DTTM) VALUES (@BATCH_CNTRL_ID, @PROCESS_ID, @PROCESS_CNTRL_STS, @PROCESS_STR_DTTM, @PROCESS_RESTR_CNTR, @PROCESS_INIT_STR_DTTM)";
                    SqlCommand insertTaskCmd = new SqlCommand(insertTask, con);
                    insertTaskCmd.Parameters.Add("@BATCH_CNTRL_ID", SqlDbType.UniqueIdentifier).Value = new Guid(BATCH_CNTRL_ID);
                    insertTaskCmd.Parameters.Add("@PROCESS_ID", SqlDbType.UniqueIdentifier).Value = new Guid(PROCESS_ID);
                    insertTaskCmd.Parameters.Add("@PROCESS_CNTRL_STS", SqlDbType.VarChar).Value = PROCESS_CNTRL_STS;
                    insertTaskCmd.Parameters.Add("@PROCESS_STR_DTTM", SqlDbType.DateTime2).Value = PROCESS_STR_DTTM;
                    insertTaskCmd.Parameters.Add("@PROCESS_RESTR_CNTR", SqlDbType.BigInt).Value = PROCESS_RESTR_CNTR;
                    insertTaskCmd.Parameters.Add("@PROCESS_INIT_STR_DTTM", SqlDbType.DateTime2).Value = PROCESS_INIT_STR_DTTM;
                    insertTaskCmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public Tuple<string, int> GetProcessControlID(string PROCESS_ID, string BATCH_CNTRL_ID)
            {
            //ABC_RULE_ID
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if (PROCESS_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (BATCH_CNTRL_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if ((ProcessStatus == 0))
                {
                //Build Where Clause

                WHERE_CLAUSE = "PROCESS_ID='" + PROCESS_ID.ToString() + "' AND BATCH_CNTRL_ID='"+ BATCH_CNTRL_ID.ToString()+"'";


                //Build Query
                string queryCMD = "SELECT PROCESS_CNTRL_ID FROM PROCESS_CONTROL WHERE " + WHERE_CLAUSE.ToString();

                using (SqlConnection con = new SqlConnection(connectString))
                    {
                    using (SqlDataAdapter resultSet = new SqlDataAdapter(queryCMD, con))
                        {
                        resultSet.Fill(dtResult);
                        }
                    }

                //Build Return Tuple
                //Tuple<PROCESS_CNTRL_ID,ProcessStatus>

                if (dtResult.Rows.Count > 0)
                    {
                    DataRow rstRow = dtResult.Rows[0];
                    var rtrnTuple = new Tuple<string, int>(rstRow["PROCESS_CNTRL_ID"].ToString(), ProcessStatus);
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus++;
                    var rtrnTuple = new Tuple<string, int>("EMPTY", ProcessStatus);
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build error Tuple
                ProcessStatus++;
                var rtrnTuple = new Tuple<String, int>("ERROR", ProcessStatus);
                return rtrnTuple;
                }

            }

        public Tuple<string, int, Tuple<string, string, string, DateTime, DateTime, int, DateTime>> GetProcessControl(string PROCESS_CNTRL_ID)
            {
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if (PROCESS_CNTRL_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if ((ProcessStatus == 0))
                {
                //Build Where Clause

                WHERE_CLAUSE = " PROCESS_CNTRL_ID ='" + PROCESS_CNTRL_ID.ToString()+ "'";


                //Build Query
                string queryCMD = "SELECT PROCESS_CNTRL_ID, BATCH_CNTRL_ID, PROCESS_ID, PROCESS_CNTRL_STS, PROCESS_STR_DTTM, PROCESS_END_DTTM, PROCESS_RESTR_CNTR, PROCESS_INIT_STR_DTTM FROM PROCESS_CONTROL WHERE " + WHERE_CLAUSE.ToString();

                using (SqlConnection con = new SqlConnection(connectString))
                    {
                    using (SqlDataAdapter resultSet = new SqlDataAdapter(queryCMD, con))
                        {
                        resultSet.Fill(dtResult);
                        }
                    }

                //Build Return Tuple
                //Tuple<PROCESS_CNTRL_ID,ProcessStatus, Tuple <BATCH_CNTRL_ID, PROCESS_ID, PROCESS_CNTRL_STS, PROCESS_STR_DDTM, PROCESS_END_DTTM, PROCESS_RESTR_CNTR, PROCESS_INIT_STR_DTTM>>

                if (dtResult.Rows.Count > 0)
                    {
                    DataRow rstRow = dtResult.Rows[0];
                    DateTime tpPROCESS_STR_DTTM;
                    DateTime.TryParse(rstRow["PROCESS_STR_DTTM"].ToString(), out tpPROCESS_STR_DTTM);
                    DateTime tpPROCESS_END_DTTM;
                    DateTime.TryParse(rstRow["PROCESS_END_DTTM"].ToString(), out tpPROCESS_END_DTTM);
                    var rtrnTuple = new Tuple<string, int, Tuple<string, string, string, DateTime, DateTime, int, DateTime>>(rstRow["PROCESS_CNTRL_ID"].ToString(), ProcessStatus, new Tuple<string, string, string, DateTime, DateTime, int, DateTime>(rstRow["BATCH_CNTRL_ID"].ToString(), rstRow["PROCESS_ID"].ToString(), rstRow["PROCESS_CNTRL_STS"].ToString(), tpPROCESS_STR_DTTM, tpPROCESS_END_DTTM, (int)rstRow["PROCESS_RESTR_CNTR"], (DateTime)rstRow["PROCESS_INIT_STR_DTTM"]));
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus++;
                    var rtrnTuple = new Tuple<string, int, Tuple<string, string, string, DateTime, DateTime, int, DateTime>>("EMPTY", ProcessStatus, new Tuple<string, string, string, DateTime, DateTime, int, DateTime>("", "", "", DateTime.Now, DateTime.Now, 0,DateTime.Now));
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus++;
                var rtrnTuple = new Tuple<String, int, Tuple<string, string, string, DateTime, DateTime, int, DateTime>>("ERROR", ProcessStatus, new Tuple<string, string, string, DateTime, DateTime, int, DateTime>("", "", "", DateTime.Now, DateTime.Now, 0, DateTime.Now));
                return rtrnTuple;
                }

            }

        public int UpdateProcessControl(string PROCESS_CNTRL_ID, string BATCH_CNTRL_ID, string PROCESS_ID, string PROCESS_CNTRL_STS, DateTime PROCESS_STR_DTTM, DateTime PROCESS_END_DTTM, Int64 PROCESS_RESTR_CNTR, DateTime PROCESS_INIT_STR_DTTM)
            {
            int ProcessStatus = 0;

            if (PROCESS_CNTRL_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (ProcessStatus == 0)
                {
                //Build Where Clause
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = "PROCESS_CNTRL_ID='" + PROCESS_CNTRL_ID.ToString()+"'";

                //Build Set Clause
                string SET_CLAUSE = "";
                string SET_COMMA = "";
                int SetCounter = 0;

                //PROCESS_CNTRL_STS
                if (PROCESS_CNTRL_STS.Trim().Length > 0)
                    {
                    SET_CLAUSE = " PROCESS_CNTRL_STS='" + PROCESS_CNTRL_STS.ToString()+"'";
                    SetCounter++;
                    }
                //BATCH_CNTRL_ID
                if (BATCH_CNTRL_ID.Trim().Length == 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + " BATCH_CNTRL_ID='"+BATCH_CNTRL_ID.ToString()+"'";
                    }
                //PROCESS_ID
                if (PROCESS_ID.Trim().Length == 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + " PROCESS_ID='"+ PROCESS_ID.ToString()+"'";
                    }

                //PROCESS_STR_DTTM
                if (PROCESS_STR_DTTM != null)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + " PROCESS_STR_DTTM='"+PROCESS_STR_DTTM.ToString()+"'";
                    }
                //PROCESS_END_DTTM
                if (PROCESS_END_DTTM != null)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + " PROCESS_END_DTTM='"+ PROCESS_END_DTTM.ToString()+"'";
                    }
                //PROCESS_RESTR_CNTR
                if (PROCESS_RESTR_CNTR > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + " PROCESS_RESTR_CNTR='"+PROCESS_RESTR_CNTR.ToString()+"'";
                    }
                //PROCESS_INIT_STR_DTTM
                if (PROCESS_INIT_STR_DTTM != null)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + " PROCESS_INIT_STR_DTTM='"+PROCESS_INIT_STR_DTTM.ToString()+"'";
                    }

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE PROCESS_CONTROL SET " + SET_CLAUSE.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        //PROCESS_PROPERTY
        //This table stores the properties for a Process that are entered during design time
        public int DefineProcessProperty(string PROCESS_ID, string PROPERTY_NM, string PROPERTY_VALUE, string PROPERTY_VALUE_TYP, string UPDATE_UID)
            {
            DateTime UPDATE_DTTM = DateTime.Now;
            int ProcessStatus = 0;

            if (PROCESS_ID.Trim().Length ==0)
                {
                ProcessStatus++;
                }
            if (PROPERTY_NM.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (PROPERTY_VALUE.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (PROPERTY_VALUE_TYP.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    string insertTask = "INSERT INTO PROCESS_PROPERTY (PROCESS_ID, PROPERTY_NM, PROPERTY_VALUE, PROPERTY_VALUE_TYP, ENABLE_IND, UPDATE_DTTM, UPDATE_UID) VALUES (@PROCESS_ID, @PROPERTY_NM, @PROPERTY_VALUE, @PROPERTY_VALUE_TYP, @ENABLE_IND, @UPDATE_DTTM, @UPDATE_UID)";
                    SqlCommand insertTaskCmd = new SqlCommand(insertTask, con);
                    insertTaskCmd.Parameters.Add("@PROCESS_ID", SqlDbType.UniqueIdentifier).Value = new Guid(PROCESS_ID);
                    insertTaskCmd.Parameters.Add("@PROPERTY_NM", SqlDbType.VarChar).Value = PROPERTY_NM;
                    insertTaskCmd.Parameters.Add("@PROPERTY_VALUE", SqlDbType.VarChar).Value = PROPERTY_VALUE;
                    insertTaskCmd.Parameters.Add("@PROPERTY_VALUE_TYP", SqlDbType.VarChar).Value = PROPERTY_VALUE_TYP;
                    insertTaskCmd.Parameters.Add("@ENABLE_IND", SqlDbType.Char).Value = 'Y';
                    insertTaskCmd.Parameters.Add("@UPDATE_DTTM", SqlDbType.DateTime2).Value = UPDATE_DTTM;
                    insertTaskCmd.Parameters.Add("@UPDATE_UID", SqlDbType.VarChar).Value = UPDATE_UID;
                    insertTaskCmd.ExecuteNonQuery();
                    }
                }


            return ProcessStatus;
            }

        public Tuple<string, string, int, Tuple<string, string, string, DateTime, string>> GetProcessProperty(string PROCESS_ID, string PROPERTY_NM)
            {
            //PROCESS_ID, PROPERTY_NM, PROPERTY_VALUE, PROPERTY_VALUE_TYP, ENABLE_IND, UPDATE_DTTM, UPDATE_UID
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if ((PROCESS_ID.Trim().Length == 0) || (PROPERTY_NM.Trim().Length == 0))
                {
                ProcessStatus++;
                }

            if ((ProcessStatus == 0))
                {
                //Build Where Clause

                WHERE_CLAUSE = " PROCESS_ID ='" + PROCESS_ID.ToString() + "' AND PROPERTY_NM LIKE '" + PROPERTY_NM.ToString().Trim() + "'";


                //Build Query
                string queryCMD = "SELECT PROCESS_ID, PROPERTY_NM, PROPERTY_VALUE, PROPERTY_VALUE_TYP, ENABLE_IND, UPDATE_DTTM, UPDATE_UID FROM PROCESS_PROPERTY WHERE " + WHERE_CLAUSE.ToString();

                using (SqlConnection con = new SqlConnection(connectString))
                    {
                    using (SqlDataAdapter resultSet = new SqlDataAdapter(queryCMD, con))
                        {
                        resultSet.Fill(dtResult);
                        }
                    }

                //Build Return Tuple
                //Tuple <PROCESS_ID,PROPERTY_NM,ProcessStatus, Tuple< PROPERTY_VALUE, PROPERTY_VALUE_TYP, ENABLE_IND, UPDATE_DTTM, UPDATE_UID>>

                if (dtResult.Rows.Count > 0)
                    {
                    DataRow rstRow = dtResult.Rows[0];
                    var rtrnTuple = new Tuple<string, string, int, Tuple<string, string, string, DateTime, string>>(rstRow["PROCESS_ID"].ToString(), rstRow["PROPERTY_NM"].ToString(), ProcessStatus, new Tuple<string, string, string, DateTime, string>(rstRow["PROPERTY_VALUE"].ToString(), rstRow["PROPERTY_VALUE_TYP"].ToString(), rstRow["ENABLE_IND"].ToString(), (DateTime)rstRow["UPDATE_DTTM"], rstRow["UPDATE_UID"].ToString()));
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus++;
                    var rtrnTuple = new Tuple<string, string, int, Tuple<string, string, string, DateTime, string>>("EMPTY", "", ProcessStatus, new Tuple<string, string, string, DateTime, string>("", "", "", DateTime.Now, ""));
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build error Tuple
                ProcessStatus++;
                var rtrnTuple = new Tuple<string, string, int, Tuple<string, string, string, DateTime, string>> ("ERROR", "", ProcessStatus, new Tuple<string, string, string, DateTime, string>("", "", "", DateTime.Now, "")); ;
                return rtrnTuple;
                }

            }

        public DataTable GetProcessProperties(string PROCESS_ID)
            {
            //PROCESS_ID, PROPERTY_NM, PROPERTY_VALUE, PROPERTY_VALUE_TYP, ENABLE_IND, UPDATE_DTTM, UPDATE_UID
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if (PROCESS_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if ((ProcessStatus == 0))
                {
                //Build Where Clause

                WHERE_CLAUSE = " PROCESS_ID ='" + PROCESS_ID.ToString() + "'";


                //Build Query
                string queryCMD = "SELECT PROCESS_ID, PROPERTY_NM, PROPERTY_VALUE, PROPERTY_VALUE_TYP, ENABLE_IND, UPDATE_DTTM, UPDATE_UID FROM PROCESS_PROPERTY WHERE " + WHERE_CLAUSE.ToString();

                using (SqlConnection con = new SqlConnection(connectString))
                    {
                    using (SqlDataAdapter resultSet = new SqlDataAdapter(queryCMD, con))
                        {
                        resultSet.Fill(dtResult);
                        }
                    }

                return dtResult;

                }
            else
                {
                return dtResult;
                }

            }


        public int EnableProcessProperty(string PROCESS_ID, string PROPERTY_NM, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'Y';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROPERTY_NM.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (PROCESS_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " PROPERTY_NM='" + PROPERTY_NM.ToString() + "' AND PROCESS_ID='" + PROCESS_ID.ToString()+"'";

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE PROCESS_PROPERTY SET ENABLE_IND='" + ENABLE_IND.ToString() + "', UPDATE_UID='" + UPDATE_UID.ToString() + "', UPDATE_DTTM='" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DisableProcessProperty(string PROCESS_ID, string PROPERTY_NM, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'N';
            string WHERE_CLAUSE = "";
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROPERTY_NM.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (PROCESS_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (ProcessStatus == 0)
                {
                WHERE_CLAUSE = " PROPERTY_NM='" + PROPERTY_NM.ToString() + "' AND PROCESS_ID='" + PROCESS_ID.ToString()+"'";


                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE PROCESS_PROPERTY SET ENABLE_IND='" + ENABLE_IND.ToString() + "', UPDATE_UID='" + UPDATE_UID.ToString() + "', UPDATE_DTTM='" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int UpdateProcessProperty(string PROCESS_ID, string PROPERTY_NM, string PROPERTY_VALUE, string PROPERTY_VALUE_TYP, string ENABLE_IND, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROCESS_ID.Trim().Length ==0)
                {
                ProcessStatus++;
                }

            if (PROPERTY_NM.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (ProcessStatus == 0)
                {
                //Build Where Clause
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = "PROCESS_ID='" + PROCESS_ID.ToString() + "', PROPERTY_NM='" + PROPERTY_NM.ToString()+"'";

                //Build Set Clause
                string SET_CLAUSE = "";
                string SET_COMMA = "";
                int SetCounter = 0;

                //PROPERTY_VALUE
                if (PROPERTY_VALUE.Trim().Length > 0)
                    {
                    SET_CLAUSE = "PROPERTY_VALUE='" + PROPERTY_VALUE.ToString()+"'";
                    SetCounter++;
                    }

                //PROPERTY_VALUE_TYP
                if (PROPERTY_VALUE_TYP.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE =SET_COMMA.ToString()+ "PROPERTY_VALUE_TYP='" + PROPERTY_VALUE_TYP.ToString()+"'";
                    }

                //ENABLE_IND
                if (ENABLE_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_COMMA.ToString() + "ENABLE_IND='" + ENABLE_IND.ToString()+"'";
                    }

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE PROCESS_PROPERTY SET " + SET_CLAUSE.ToString() + ", UPDATE_UID='" + UPDATE_UID.ToString() + "', UPDATE_DTTM='" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DeleteProcessProperty(string PROCESS_ID, string PROPERTY_NM)
            {
            int ProcessStatus = 0;
            if ((PROCESS_ID.Trim().Length == 0) || (PROPERTY_NM.Trim().Length == 0))
                {
                ProcessStatus++;
                }
            else
                {
                string WHERE_CLAUSE = " PROCESS_ID ='" + PROCESS_ID.ToString() + "' AND PROPERTY_NM LIKE '" + PROPERTY_NM.ToString().Trim() + "'";
                using (var dConn = new SqlConnection(connectString))
                using (var delCmd = dConn.CreateCommand())
                    {
                    dConn.Open();
                    delCmd.CommandText = "DELETE FROM PROCESS_PROPERTY WHERE" + WHERE_CLAUSE;
                    delCmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        //PROCESS_CONTROL_STATS
        //This table stores the metadata of the execution as passed from the Process
        public int DefineProcessControlStats(string PROCESS_CNTRL_ID, string PROCESS_CNTRL_STAT_NM, string PROCESS_CNTRL_STAT_DESC, string PROCESS_CNTRL_STAT_TYP, string PROCESS_CNTRL_STAT_VALU, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;
            if (PROCESS_CNTRL_ID.Trim().Length ==0)
                {
                ProcessStatus++;
                }
            if (PROCESS_CNTRL_STAT_NM.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    string insertTask = "INSERT INTO PROCESS_CONTROL_STATS (PROCESS_CNTRL_ID, PROCESS_CNTRL_STAT_NM , PROCESS_CNTRL_STAT_DESC, PROCESS_CNTRL_STAT_TYP,PROCESS_CNTRL_STAT_VALU, UPDATE_DTTM, UPDATE_UID) VALUES (@PROCESS_CNTRL_ID, @PROCESS_CNTRL_STAT_NM, @PROCESS_CNTRL_STAT_DESC, @PROCESS_CNTRL_STAT_TYP, @PROCESS_CNTRL_STAT_VALU, @UPDATE_DTTM,@UPDATE_UID)";
                    SqlCommand insertTaskCmd = new SqlCommand(insertTask, con);
                    insertTaskCmd.Parameters.Add("@PROCESS_CNTRL_ID", SqlDbType.UniqueIdentifier).Value = new Guid(PROCESS_CNTRL_ID);
                    insertTaskCmd.Parameters.Add("@PROCESS_CNTRL_STAT_NM", SqlDbType.VarChar).Value = PROCESS_CNTRL_STAT_NM;
                    insertTaskCmd.Parameters.Add("@PROCESS_CNTRL_STAT_DESC", SqlDbType.VarChar).Value = PROCESS_CNTRL_STAT_DESC;
                    insertTaskCmd.Parameters.Add("@PROCESS_CNTRL_STAT_TYP", SqlDbType.VarChar).Value = PROCESS_CNTRL_STAT_TYP;
                    insertTaskCmd.Parameters.Add("@PROCESS_CNTRL_STAT_VALU", SqlDbType.VarChar).Value = PROCESS_CNTRL_STAT_VALU;
                    insertTaskCmd.Parameters.Add("@UPDATE_DTTM", SqlDbType.DateTime2).Value = UPDATE_DTTM;
                    insertTaskCmd.Parameters.Add("@UPDATE_UID", SqlDbType.VarChar).Value = UPDATE_UID;
                    insertTaskCmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public Tuple<string, string, string, string, string, DateTime, string, Tuple<int> > GetProcessControlStats(string PROCESS_CNTRL_ID, string PROCESS_CNTRL_STAT_NM)
            {
            //int PROCESS_CNTRL_ID, string PROCESS_CNTRL_STAT_NM , string PROCESS_CNTRL_STAT_DESC, string PROCESS_CNTRL_STAT_TYP, string PROCESS_CNTRL_STAT_VALU ,string UPDATE_UID
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if (PROCESS_CNTRL_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (PROCESS_CNTRL_STAT_NM.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if ((ProcessStatus == 0))
                {
                //Build Where Clause

                WHERE_CLAUSE = " PROCESS_CNTRL_ID ='" + PROCESS_CNTRL_ID.ToString() + "' AND PROCESS_CNTRL_STAT_NM='" + PROCESS_CNTRL_STAT_NM.ToString()+"'";


                //Build Query
                string queryCMD = "SELECT PROCESS_CNTRL_ID, PROCESS_CNTRL_STAT_NM , PROCESS_CNTRL_STAT_DESC, PROCESS_CNTRL_STAT_TYP,PROCESS_CNTRL_STAT_VALU, UPDATE_DTTM, UPDATE_UID FROM PROCESS_CONTROL_STATS WHERE " + WHERE_CLAUSE.ToString();

                using (SqlConnection con = new SqlConnection(connectString))
                    {
                    using (SqlDataAdapter resultSet = new SqlDataAdapter(queryCMD, con))
                        {
                        resultSet.Fill(dtResult);
                        }
                    }

                //Build Return Tuple
                //Tuple<>

                if (dtResult.Rows.Count > 0)
                    {
                    DataRow rstRow = dtResult.Rows[0];
                    var rtrnTuple = new Tuple<string, string, string, string, string, DateTime, string,Tuple <int>>(rstRow["PROCESS_CNTRL_ID"].ToString(), rstRow["PROCESS_CNTRL_STAT_DESC"].ToString(), rstRow["PROCESS_CNTRL_STAT_NM"].ToString(), rstRow["PROCESS_CNTRL_STAT_TYP"].ToString(), rstRow["PROCESS_CNTRL_STAT_VALU"].ToString(), (DateTime)rstRow["UPDATE_DTTM"], rstRow["UPDATE_UID"].ToString(),new Tuple<int>(ProcessStatus));
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus++;
                    var rtrnTuple = new Tuple<string, string, string, string, string, DateTime, string, Tuple<int>>("EMPTY", PROCESS_CNTRL_ID.ToString(), "", PROCESS_CNTRL_STAT_NM.ToString(), "", DateTime.Now, "", new Tuple<int>(ProcessStatus));
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus++;
                var rtrnTuple = new Tuple<string, string, string, string, string, DateTime, string, Tuple<int>>("ERROR", PROCESS_CNTRL_ID.ToString(), "", PROCESS_CNTRL_STAT_NM.ToString(), "", DateTime.Now, "", new Tuple<int>(ProcessStatus));
                return rtrnTuple;
                }
            }

        public int UpdateProcessControlStats(string PROCESS_CNTRL_ID, string PROCESS_CNTRL_STAT_NM, string PROCESS_CNTRL_STAT_DESC, string PROCESS_CNTRL_STAT_TYP, string PROCESS_CNTRL_STAT_VALU, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROCESS_CNTRL_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (PROCESS_CNTRL_STAT_NM.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (ProcessStatus == 0)
                {
                //Build Where Clause
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " PROCESS_CNTRL_ID ='" + PROCESS_CNTRL_ID.ToString() + "' AND PROCESS_CNTRL_STAT_NM='" + PROCESS_CNTRL_STAT_NM.ToString()+"'";

                //Build Set Clause
                string SET_CLAUSE = "";
                string SET_COMMA = "";
                int SetCounter = 0;

                //PROCESS_CNTRL_STAT_DESC
                if (PROCESS_CNTRL_STAT_DESC.Trim().Length > 0)
                    {
                    SET_CLAUSE = "PROCESS_CNTRL_STAT_DESC='" + PROCESS_CNTRL_STAT_DESC.ToString()+"'";
                    SetCounter++;
                    }
                //PROCESS_CNTRL_STAT_TYP
                if (PROCESS_CNTRL_STAT_TYP.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + "PROCESS_CNTRL_STAT_TYP='"+PROCESS_CNTRL_STAT_TYP.ToString()+"'";
                    }
                //PROCESS_CNTRL_STAT_VALU
                if (PROCESS_CNTRL_STAT_VALU.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + "PROCESS_CNTRL_STAT_VALU='"+ PROCESS_CNTRL_STAT_VALU.ToString()+"'";
                    }


                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE PROCESS_CONTROL_STATS SET " + SET_CLAUSE.ToString() + ", UPDATE_UID='" + UPDATE_UID.ToString() + "', UPDATE_DTTM='" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        //PROCESS_ABC
        //This table stores the results of the Audit Balance and Control measuremnts for each Process for each run
        public int StoreProcessABCResults(string PROCESS_ID, string PROCESS_CNTRL_ID, string ABC_RULE_ID, string EXPECTED_VALUE, string ACTUAL_VALUE, string ABC_PASS_IND, string PROCESS_BALANCE_IND, string PROCESS_CONTROL_IND, string PROCESS_AUDIT_IND, string CRITICAL_FAIL_IND, string UPDATE_UID)
            {
            DateTime UPDATE_DTTM = DateTime.Now;
            int ProcessStatus = 0;

            if (PROCESS_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (PROCESS_CNTRL_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (ABC_RULE_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (EXPECTED_VALUE.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (ACTUAL_VALUE.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (ABC_PASS_IND.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (PROCESS_BALANCE_IND.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (PROCESS_CONTROL_IND.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (PROCESS_AUDIT_IND.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (CRITICAL_FAIL_IND.Trim().Length == 0)
                {
                ProcessStatus++;
                }
            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    string insertTask = "INSERT INTO PROCESS_ABC (PROCESS_ID, PROCESS_CNTRL_ID, ABC_RULE_ID,EXPECTED_VALUE, ACTUAL_VALUE,  ABC_PASS_IND, PROCESS_BALANCE_IND, PROCESS_CONTROL_IND, PROCESS_AUDIT_IND, CRITICAL_FAIL_IND, UPDATE_DTTM, UPDATE_UID) VALUES (@PROCESS_ID, @PROCESS_CNTRL_ID, @ABC_RULE_ID,@EXPECTED_VALUE, @ACTUAL_VALUE, @ABC_PASS_IND, @PROCESS_BALANCE_IND, @PROCESS_CONTROL_IND, @PROCESS_AUDIT_IND, @CRITICAL_FAIL_IND, @UPDATE_DTTM, @UPDATE_UID)";
                    SqlCommand insertTaskCmd = new SqlCommand(insertTask, con);
                    insertTaskCmd.Parameters.Add("@PROCESS_ID", SqlDbType.UniqueIdentifier).Value = new Guid(PROCESS_ID);
                    insertTaskCmd.Parameters.Add("@PROCESS_CNTRL_ID", SqlDbType.UniqueIdentifier).Value = new Guid(PROCESS_CNTRL_ID);
                    insertTaskCmd.Parameters.Add("@ABC_RULE_ID", SqlDbType.UniqueIdentifier).Value = new Guid(ABC_RULE_ID);
                    insertTaskCmd.Parameters.Add("@EXPECTED_VALUE", SqlDbType.VarChar).Value = EXPECTED_VALUE;
                    insertTaskCmd.Parameters.Add("@ACTUAL_VALUE", SqlDbType.VarChar).Value = ACTUAL_VALUE;
                    insertTaskCmd.Parameters.Add("@ABC_PASS_IND", SqlDbType.VarChar).Value = ABC_PASS_IND;
                    insertTaskCmd.Parameters.Add("@PROCESS_BALANCE_IND", SqlDbType.VarChar).Value = PROCESS_BALANCE_IND;
                    insertTaskCmd.Parameters.Add("@PROCESS_CONTROL_IND", SqlDbType.VarChar).Value = PROCESS_CONTROL_IND;
                    insertTaskCmd.Parameters.Add("@PROCESS_AUDIT_IND", SqlDbType.VarChar).Value = PROCESS_AUDIT_IND;
                    insertTaskCmd.Parameters.Add("@CRITICAL_FAIL_IND", SqlDbType.VarChar).Value = CRITICAL_FAIL_IND;
                    insertTaskCmd.Parameters.Add("@UPDATE_DTTM", SqlDbType.DateTime2).Value = UPDATE_DTTM;
                    insertTaskCmd.Parameters.Add("@UPDATE_UID", SqlDbType.VarChar).Value = UPDATE_UID;
                    insertTaskCmd.ExecuteNonQuery();
                    }
                }


            return ProcessStatus;
            }

        public Tuple<string, string, string, DateTime, string, int, Tuple<string, string, string, string, string, string>> GetProcessABCResult(string PROCESS_ID, string PROCESS_CNTRL_ID, string ABC_RULE_ID)
            {
            //PROCESS_ID, PROCESS_CNTRL_ID, ABC_RULE_ID, EXPECTED_VALUE, ACTUAL_VALUE, ABC_PASS_IND, PROCESS_BALANCE_IND, PROCESS_AUDIT_IND, CRITICAL_FAIL_IND, UPDATE_DTTM, UPDATE_UID
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if ((PROCESS_ID.Trim().Length == 0) || (PROCESS_CNTRL_ID.Trim().Length == 0) || (ABC_RULE_ID.Trim().Length == 0))
                {
                ProcessStatus++;
                }

            if ((ProcessStatus == 0))
                {
                //Build Where Clause

                WHERE_CLAUSE = " PROCESS_ID ='" + PROCESS_ID.ToString() + "' AND PROCESS_CNTRL_ID ='" + PROCESS_CNTRL_ID.ToString() + "' AND ABC_RULE_ID ='" + ABC_RULE_ID.ToString() + "'";


                //Build Query
                string queryCMD = "SELECT PROCESS_ID, PROCESS_CNTRL_ID, ABC_RULE_ID, EXPECTED_VALUE, ACTUAL_VALUE, ABC_PASS_IND, PROCESS_BALANCE_IND, PROCESS_AUDIT_IND, CRITICAL_FAIL_IND, UPDATE_DTTM, UPDATE_UID FROM PROCESS_ABC WHERE " + WHERE_CLAUSE.ToString();

                using (SqlConnection con = new SqlConnection(connectString))
                    {
                    using (SqlDataAdapter resultSet = new SqlDataAdapter(queryCMD, con))
                        {
                        resultSet.Fill(dtResult);
                        }
                    }

                //Build Return Tuple
                //Tuple <PROCESS_ID, PROCESS_CNTRL_ID, ABC_RULE_ID, UPDATE_DTTM, UPDATE_UID,ProcessStatus, Tuple <EXPECTED_VALUE, ACTUAL_VALUE, ABC_PASS_IND, PROCESS_BALANCE_IND, PROCESS_AUDIT_IND, CRITICAL_FAIL_IND>>

                if (dtResult.Rows.Count > 0)
                    {
                    DataRow rstRow = dtResult.Rows[0];
                    var rtrnTuple = new Tuple<string, string, string, DateTime, string, int, Tuple<string, string, string, string, string, string>>(rstRow["PROCESS_ID"].ToString(), rstRow["PROCESS_CNTRL_ID"].ToString(), rstRow["ABC_RULE_ID"].ToString(), (DateTime)rstRow["UPDATE_DTTM"], rstRow["UPDATE_UID"].ToString(), ProcessStatus, new Tuple<string, string, string, string, string, string>(rstRow["EXPECTED_VALUE"].ToString(), rstRow["ACTUAL_VALUE"].ToString(), rstRow["ABC_PASS_IND"].ToString(), rstRow["PROCESS_BALANCE_IND"].ToString(), rstRow["PROCESS_AUDIT_IND"].ToString(), rstRow["CRITICAL_FAIL_IND"].ToString()));
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus++;
                    var rtrnTuple = new Tuple<string, string, string, DateTime, string, int, Tuple<string, string, string, string, string, string>>("Empty", "", "", DateTime.Now, "", ProcessStatus, new Tuple<string, string, string, string, string, string>("", "", "", "", "", ""));
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus++;
                var rtrnTuple = new Tuple<string, string, string, DateTime, string, int, Tuple<string, string, string, string, string, string>>("ERROR","", "", DateTime.Now, "", ProcessStatus, new Tuple<string, string, string, string, string, string>("", "", "", "", "", ""));
                return rtrnTuple;
                }

            }

        public int UpdateProcessABCResult(string PROCESS_ID, string PROCESS_CNTRL_ID, string ABC_RULE_ID, string EXPECTED_VALUE, string ACTUAL_VALUE, string ABC_PASS_IND, string PROCESS_BALANCE_IND, string PROCESS_CONTROL_IND, string PROCESS_AUDIT_IND, string CRITICAL_FAIL_IND, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROCESS_ID.Trim().Length ==0)
                {
                ProcessStatus++;
                }

            if (PROCESS_CNTRL_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (ABC_RULE_ID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus++;
                }

            if (ProcessStatus == 0)
                {
                //Build Where Clause
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = "PROCESS_ID='" + PROCESS_ID.ToString() + "' AND PROCESS_CNTRL_ID='" + PROCESS_CNTRL_ID.ToString() + "' AND ABC_RULE_ID='" + ABC_RULE_ID.ToString()+"'";

                //Build Set Clause
                string SET_CLAUSE = "";
                string SET_COMMA = "";
                int SetCounter = 0;

                //EXPECTED_VALUE
                if (EXPECTED_VALUE.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = "EXPECTED_VALUE='" + EXPECTED_VALUE.ToString()+"'";
                    }

                //ACTUAL_VALUE
                if (ACTUAL_VALUE.Trim().Length > 0)
                    {
                    if (SetCounter > 0) {
                        SET_CLAUSE = SET_COMMA.ToString() + "ACTUAL_VALUE='" + ACTUAL_VALUE.ToString() + "'";
                        }
                    else {
                        SET_COMMA = ", ";
                        SetCounter++;
                        SET_CLAUSE = " ACTUAL_VALUE='" + ACTUAL_VALUE.ToString() + "'";
                        }
                    }

                //ABC_PASS_IND
                if (ABC_PASS_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) {
                        SET_CLAUSE = SET_COMMA.ToString() + "ABC_PASS_IND='" + ABC_PASS_IND.ToString() + "'";
                        } else {
                        SET_COMMA = ", ";
                        SET_CLAUSE =" ABC_PASS_IND='" + ABC_PASS_IND.ToString() + "'";
                        SetCounter++; }
                     }

                //PROCESS_BALANCE_IND
                if (PROCESS_BALANCE_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) {
                        SET_CLAUSE = SET_COMMA.ToString() + "PROCESS_BALANCE_IND='" + PROCESS_BALANCE_IND.ToString() + "'";
                        }
                    else { SetCounter++;
                        SET_COMMA = ", ";
                        SET_CLAUSE = " PROCESS_BALANCE_IND='" + PROCESS_BALANCE_IND.ToString() + "'";
                        }
                    }

                //PROCESS_CONTROL_IND
                if (PROCESS_CONTROL_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) {
                        SET_CLAUSE = SET_COMMA.ToString() + "PROCESS_CONTROL_IND='" + PROCESS_CONTROL_IND.ToString() + "'";
                        }
                    else {
                        SET_COMMA = ", ";
                        SetCounter++;
                        SET_CLAUSE = " PROCESS_CONTROL_IND='" + PROCESS_CONTROL_IND.ToString() + "'";
                        }
                    }

                //PROCESS_AUDIT_IND
                if (PROCESS_AUDIT_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) {
                        SET_CLAUSE = SET_COMMA.ToString() + "PROCESS_AUDIT_IND='" + PROCESS_AUDIT_IND.ToString() + "'";
                        }
                    else {
                        SET_COMMA = ", ";
                        SetCounter++;
                        SET_CLAUSE = " PROCESS_AUDIT_IND='" + PROCESS_AUDIT_IND.ToString() + "'";
                        }
                    }

                //CRITICAL_FAIL_IND
                if (CRITICAL_FAIL_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) {
                        SET_CLAUSE = SET_COMMA.ToString() + "CRITICAL_FAIL_IND='" + CRITICAL_FAIL_IND.ToString() + "'";
                        }
                    else {
                        SET_CLAUSE = " CRITICAL_FAIL_IND='" + CRITICAL_FAIL_IND.ToString() + "'";
                        }
                    }

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE PROCESS_ABC SET " + SET_CLAUSE.ToString() + ", UPDATE_UID='" + UPDATE_UID.ToString() + "', UPDATE_DTTM='" + UPDATE_DTTM.ToString() + "' WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        //
        public void Dispose()
        {
            //
        }
        
    }
}
