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
        public string connectString;
      
        public ABC()
        {
            
            string fContent = "", cItem = "", cValue = "",tString="", path=@"..\ABCConfig.txt";
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
                    string[] split_words = lines[i].Split(new char[] { '=' });
                    cItem = split_words.ToString().Trim();
                    cValue = split_words.ToString().Trim();
                    configuration.Add(cItem, cValue);
                }
                configuration.TryGetValue("ConnectionString", out tString);
                connectString = tString.ToString().Trim();
            }
            catch (Exception ex)
            {
                return;
            }

        }
        //APPLICATION
        //This table stores the information about the Application
        public int DefineApplication(string APP_NAME, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'Y';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (APP_NAME.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "INSERT INTO APPLICATION (APP_NAME, ENABLED_IND,UPDATE_DTTM,UPDATE_UID) VALUES ('" + APP_NAME + "', '" + ENABLE_IND + "', " + UPDATE_DTTM + "'," + UPDATE_UID + "')";
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public Tuple<int, string, string, DateTime, string, int> GetApplication(int APP_ID, string APP_NAME)
            {
            //APP_ID, APP_NAME, ENABLE_IND, UPDATE_DTTM, UPDATE_UUID, ProcessStatus
            int ProcessStatus = 0;
            int WhereProcess = 0;
            string WHERE_APP_ID = "", WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if (APP_ID <= 0)
                {
                WhereProcess = 1;
                }
            if (APP_NAME.Trim().Length == 0)
                {
                 WhereProcess++;
                }
            if (WhereProcess == 2)
                {
                ProcessStatus = 1;
            }
            if ((ProcessStatus == 0) && (WhereProcess<2))
                {
                //Build Where Clause
                if (APP_ID > 0)
                    {
                    WHERE_APP_ID = " APP_ID =" + APP_ID.ToString();
                    }

                if ((APP_ID > 0) && (APP_NAME.Trim().Length > 0))
                    {
                    WHERE_CLAUSE = WHERE_APP_ID + "AND  APP_NAME =" + APP_NAME.ToString();
                    }
                else
                    {
                    WHERE_CLAUSE = " APP_NAME =" + APP_NAME.ToString();
                    }

                //Build Query
                string queryCMD = "SELECT APP_ID, APP_NAME, ENABLE_IND, UPDATE_DTTM, UPDATE_UUID FROM APPLICATION WHERE " + WHERE_CLAUSE.ToString();

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
                    var rtrnTuple = new Tuple<int, string, string, DateTime, string, int>((int)rstRow["APP_ID"], rstRow["APP_NAME"].ToString(), rstRow["ENABLE_IND"].ToString(), (DateTime)rstRow["UPDATE_DTTM"], rstRow["UPDATE_UID"].ToString(), ProcessStatus);
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus = 1;
                    var rtrnTuple = new Tuple<int, string, string, DateTime, string, int>(0, "", "", DateTime.Now, "", ProcessStatus);
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus = 1;
                var rtrnTuple = new Tuple<int, string, string, DateTime, string, int>(0, "", "", DateTime.Now, "", ProcessStatus);
                return rtrnTuple;
                }

                }

        public int EnableApplication(int APP_ID, string APP_NAME, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'Y';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (APP_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " APP_ID=" + APP_ID.ToString();
                if (APP_NAME.Length > 0)
                    {
                    WHERE_CLAUSE = WHERE_CLAUSE + "AND  APP_NAME=" + APP_NAME.ToString();
                    }

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE APPLICATION SET ENABLE_IND=" + ENABLE_IND.ToString()+", UPDATE_UUID=" + UPDATE_UID.ToString() + "UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DisableApplication(int APP_ID, string APP_NAME, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'N';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (APP_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " APP_ID=" + APP_ID.ToString();
                if (APP_NAME.Length > 0)
                    {
                    WHERE_CLAUSE = WHERE_CLAUSE + "AND  APP_NAME=" + APP_NAME.ToString();
                    }

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE APPLICATION SET ENABLE_IND=" + ENABLE_IND.ToString()+", UPDATE_UUID=" + UPDATE_UID.ToString() + "UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int UpdateApplication(int APP_ID, string APP_NAME, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'Y';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (APP_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (APP_NAME.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " APP_ID=" + APP_ID.ToString();

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE APPLICATION SET APP_NAME=" + APP_NAME.ToString() + ", ENABLE_IND=" + ENABLE_IND.ToString()+ ", UPDATE_UUID=" + UPDATE_UID.ToString() + "UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DeleteApplication(int APP_ID)
            {
            int ProcessStatus = 0;
            if (APP_ID <= 0)
                {
                ProcessStatus++;
                }

            if (ProcessStatus == 0)
                {
                using (var dConn = new SqlConnection(connectString))
                using (var delCmd = dConn.CreateCommand())
                    {

                    delCmd.CommandText = "DELETE FROM APPLICATION WHERE APP_ID = @APP_ID";
                    delCmd.Parameters.AddWithValue("@APP_ID", APP_ID.ToString());
                    delCmd.ExecuteNonQuery();
                    }

                }
            return ProcessStatus;
            }

        //BATCH
        //This table stores the information about the Batch
        public int DefineBatch(string BATCH_TYPE, string BATCH_NM, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'Y';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (BATCH_TYPE.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (BATCH_NM.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "INSERT INTO BATCH (BATCH_TYPE, BATCH_NM,ENABLED_IND,UPDATE_DTTM,UPDATE_UID) VALUES ('" + BATCH_TYPE + "', '" + BATCH_NM + "', " + "', '" + ENABLE_IND + "', " + UPDATE_DTTM + "'," + UPDATE_UID + "')";
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public Tuple<int, string, string, string, DateTime, string, int> GetBatch(int BATCH_ID, string BATCH_NM)
            {
            //BATCH_ID, BATCH_NM, BATCH_TYPE,ENABLE_IND, UPDATE_DTTM, UPDATE_UUID, ProcessStatus
            int ProcessStatus = 0;
            int WhereProcess = 0;
            string WHERE_BATCH_ID = "", WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if (BATCH_ID <= 0)
                {
                WhereProcess = 1;
                }
            if (BATCH_NM.Trim().Length == 0)
                {
                WhereProcess++;
                }
            if (WhereProcess == 2)
                {
                ProcessStatus = 1;
                }
            if ((ProcessStatus == 0) && (WhereProcess < 2))
                {
                //Build Where Clause
                if (BATCH_ID > 0)
                    {
                    WHERE_BATCH_ID = " BATCH_ID =" + BATCH_ID.ToString();
                    }

                if ((BATCH_ID > 0) && (BATCH_NM.Length > 0))
                    {
                    WHERE_CLAUSE = WHERE_BATCH_ID + "AND  BATCH_NM =" + BATCH_NM.ToString();
                    }
                else
                    {
                    WHERE_CLAUSE = " BATCH_NM =" + BATCH_NM.ToString();
                    }

                //Build Query
                string queryCMD = "SELECT BATCH_ID, BATCH_NM, BATCH_TYPE,ENABLE_IND, UPDATE_DTTM, UPDATE_UUID FROM BATCH WHERE " + WHERE_CLAUSE.ToString();

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
                    var rtrnTuple = new Tuple<int, string, string, string, DateTime, string, int>((int)rstRow["BATCH_ID"], rstRow["BATCH_NM"].ToString(), rstRow["BATCH_TYPE"].ToString(), rstRow["ENABLE_IND"].ToString(), (DateTime)rstRow["UPDATE_DTTM"], rstRow["UPDATE_UID"].ToString(), ProcessStatus);
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus = 1;
                    var rtrnTuple = new Tuple<int, string, string, string, DateTime, string, int>(0, "", "", "", DateTime.Now, "", ProcessStatus);
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus = 1;
                var rtrnTuple = new Tuple<int, string, string, string, DateTime, string, int>(0, "", "", "", DateTime.Now, "", ProcessStatus);
                return rtrnTuple;
                }

            }

        public int EnableBatch(int BATCH_ID, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'Y';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (BATCH_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " BATCH_ID=" + BATCH_ID.ToString();

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE BATCH SET ENABLE_IND=" + ENABLE_IND.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + ", UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DisableBatch(int BATCH_ID, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'N';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (BATCH_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " BATCH_ID=" + BATCH_ID.ToString();


                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE BATCH SET ENABLE_IND=" + ENABLE_IND.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + ", UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int UpdateBatch(int BATCH_ID, string BATCH_NM, string BATCH_TYPE, string ENABLE_IND, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (BATCH_ID <= 0)
                {
                ProcessStatus = 1;
                }

            if ((BATCH_NM.Trim().Length == 0) && (BATCH_TYPE.Trim().Length == 0))
                {
                ProcessStatus = 1;
                }

            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus = 1;
                }

            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " BATCH_ID=" + BATCH_ID.ToString();
                string SET_CLAUSE = "";
                if (BATCH_NM.Trim().Length > 0)
                    {
                    if (BATCH_TYPE.Trim().Length > 0)
                        {
                        SET_CLAUSE = " BATCH_NM=" + BATCH_NM.ToString() + ", BATCH_TYPE="+BATCH_TYPE.ToString()+" ";
                    } else
                 {
                        SET_CLAUSE = " BATCH_NM=" + BATCH_NM.ToString() + " ";
                        }
                    } else
                {
                    SET_CLAUSE = " BATCH_TYPE="+BATCH_TYPE.ToString()+" ";

                }
                if (ENABLE_IND.Trim().Length>0) { SET_CLAUSE = SET_CLAUSE + "ENABLE_IND=" + ENABLE_IND.ToString(); }

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE APPLICATION SET " + SET_CLAUSE.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + "UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DeleteBatch (int BATCH_ID)
            {
            int ProcessStatus = 0;
            if (BATCH_ID <= 0)
                {
                ProcessStatus++;
                }
            else
                {
                using (var dConn = new SqlConnection(connectString))
                using (var delCmd = dConn.CreateCommand())
                    {
                    dConn.Open();
                    delCmd.CommandText = "DELETE FROM BATCH WHERE BATCH_ID = @BATCH_ID";
                    delCmd.Parameters.AddWithValue("@BATCH_ID", BATCH_ID.ToString());
                    delCmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        //BATCH_CONTROL
        //This table stores the information about each Batch execution
        public int InvokeBatchControl(int BATCH_ID, string BATCH_CNTRL_NM, DateTime BATCH_CNTRL_DT, char BATCH_CNTRL_ACTV_IND, DateTime BATCH_STRT_DTTM, DateTime BOUNDARY_START_DT, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (BATCH_CNTRL_NM.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (BATCH_CNTRL_ACTV_IND.ToString().Length == 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    string insertTask = "INSERT INTO BATCH_CONTROL (BATCH_ID, BATCH_CNTRL_NM,  BATCH_CNTRL_DT, BATCH_CNTRL_ACTV_IND, BATCH_STRT_DTTM, BOUNDARY_START_DT, UPDATE_DTTM,UPDATE_UID) VALUES VALUES (@BATCH_ID, @BATCH_CNTRL_NM, @BATCH_CNTRL_DT, @BATCH_CNTRL_ACTV_IND, @BATCH_STRT_DTTM, @BOUNDARY_START_DT, @UPDATE_DTTM, @UPDATE_UID)";
                    System.Data.SqlClient.SqlCommand insertTaskCmd = new System.Data.SqlClient.SqlCommand(insertTask, con);
                    insertTaskCmd.Parameters.Add("@BATCH_ID", SqlDbType.Int).Value = BATCH_ID;
                    insertTaskCmd.Parameters.Add("@BATCH_CNTRL_NM", SqlDbType.VarChar).Value = BATCH_CNTRL_NM;
                    insertTaskCmd.Parameters.Add("@BATCH_CNTRL_DT", SqlDbType.DateTime2).Value = BATCH_CNTRL_DT;
                    insertTaskCmd.Parameters.Add("@BATCH_CNTRL_ACTV_IND", SqlDbType.Char).Value = BATCH_CNTRL_ACTV_IND;
                    insertTaskCmd.Parameters.Add("@BATCH_STRT_DTTM", SqlDbType.DateTime2).Value = BATCH_STRT_DTTM;
                    insertTaskCmd.Parameters.Add("@BOUNDARY_START_DT", SqlDbType.DateTime2).Value = BOUNDARY_START_DT;
                    insertTaskCmd.Parameters.Add("@UPDATE_DTTM", SqlDbType.DateTime2).Value = UPDATE_DTTM;
                    insertTaskCmd.Parameters.Add("@UPDATE_UID", SqlDbType.VarChar).Value = UPDATE_UID;
                    insertTaskCmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public Tuple<int, string, int, DateTime, string, string, int,Tuple<DateTime, DateTime, DateTime, DateTime, DateTime> > GetBatchControl(int BATCH_CNTRL_ID, string BATCH_CNTRL_NM)
            {
            int ProcessStatus = 0;
            int WhereProcess = 0;
            string WHERE_BATCH_CNTRL_ID = "", WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if (BATCH_CNTRL_ID <= 0)
                {
                WhereProcess = 1;
                }
            if (BATCH_CNTRL_NM.Length == 0)
                {
                WhereProcess++;
                }
            if (WhereProcess == 2)
                {
                ProcessStatus = 1;
                }
            if ((ProcessStatus == 0) && (WhereProcess < 2))
                {
                //Build Where Clause
                if (BATCH_CNTRL_ID > 0)
                    {
                    WHERE_BATCH_CNTRL_ID = " BATCH_CNTRL_ID =" + BATCH_CNTRL_ID.ToString();
                    }

                if ((BATCH_CNTRL_ID > 0) && (BATCH_CNTRL_NM.Length > 0))
                    {
                    WHERE_CLAUSE = WHERE_BATCH_CNTRL_ID + "AND  BATCH_CNTRL_NM =" + BATCH_CNTRL_NM.ToString();
                    }
                else
                    {
                    WHERE_CLAUSE = " BATCH_CNTRL_NM =" + BATCH_CNTRL_NM.ToString();
                    }

                //Build Query
                string queryCMD = "SELECT BATCH_CNTRL_ID, BATCH_CNTRL_NM, BATCH_ID, BATCH_CNTRL_DT, BATCH_CNTRL_ACTV_IND, BATCH_START_DT, BATCH_END_DT,BOUNDARY_START_DT, BOUNDARY_END_DT,UPDATE_DTTM, UPDATE_UUID FROM BATCH_CONTROL WHERE " + WHERE_CLAUSE.ToString();

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
                    var rtrnTuple = new Tuple<int, string, int, DateTime, string, string, int, Tuple<DateTime, DateTime, DateTime, DateTime, DateTime>>((int)rstRow["BATCH_CNTRL_ID"], rstRow["BATCH_CNTRL_NM"].ToString(), (int)rstRow["BATCH_ID"], (DateTime)rstRow["BATCH_CNTRL_DT"], rstRow["BATCH_CNTRL_ACTV_IND"].ToString(), rstRow["UPDATE_UID"].ToString(), ProcessStatus,new Tuple<DateTime,DateTime,DateTime,DateTime,DateTime>((DateTime)rstRow["BATCH_START_DTTM"], (DateTime)rstRow["BATCH_END_DTTM"], (DateTime)rstRow["BOUNDARY_START_DT"], (DateTime)rstRow["BOUNDARY_END_DT"], (DateTime)rstRow["UPDATE_DTTM"]));
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus = 1;
                    var rtrnTuple = new Tuple<int, string, int, DateTime, string, string, int, Tuple<DateTime, DateTime, DateTime, DateTime, DateTime>>(0, "", 0, DateTime.Now, "", "", ProcessStatus, new Tuple<DateTime, DateTime, DateTime, DateTime, DateTime>(DateTime.Now, DateTime.Now, DateTime.Now, DateTime.Now, DateTime.Now));
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus = 1;
                var rtrnTuple = new Tuple<int, string, int, DateTime, string, string, int, Tuple<DateTime, DateTime, DateTime, DateTime, DateTime>>(0, "", 0, DateTime.Now, "", "", ProcessStatus, new Tuple<DateTime, DateTime, DateTime, DateTime, DateTime>(DateTime.Now, DateTime.Now, DateTime.Now, DateTime.Now, DateTime.Now));
                return rtrnTuple;
                }

            }

        public int EnableBatchControl(int BATCH_CNTRL_ID, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'Y';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (BATCH_CNTRL_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " BATCH_CNTRL_ID=" + BATCH_CNTRL_ID.ToString();

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE BATCH_CONTROL SET ENABLE_IND=" + ENABLE_IND.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + ", UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DisableBatchControl(int BATCH_CNTRL_ID, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'N';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (BATCH_CNTRL_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " BATCH_CNTRL_ID=" + BATCH_CNTRL_ID.ToString();


                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE BATCH_CONTROL SET ENABLE_IND=" + ENABLE_IND.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + ", UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int UpdateBatchControl(int BATCH_CNTRL_ID, string BATCH_CNTRL_NM, DateTime BATCH_CNTRL_DT, string BATCH_CNTRL_ACTV_IND, DateTime BATCH_START_DT, DateTime BATCH_END_DT, DateTime BOUNDARY_START_DT, DateTime BOUNDARY_END_DT, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (BATCH_CNTRL_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus = 1;
                }

            if (ProcessStatus == 0)
                {
                //Build Where Clause
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " BATCH_CNTRL_ID=" + BATCH_CNTRL_ID.ToString();

                //Build Set Clause
                string SET_CLAUSE = "";
                string SET_COMMA = " ";
                int SetCounter = 0;

                //BATCH_CNTRL_NM
                if (BATCH_CNTRL_NM.Trim().Length > 0)
                    {
                    SET_CLAUSE = "BATCH_CNTRL_NM=" + BATCH_CNTRL_NM.ToString();
                    SetCounter++;
                    }
                //BATCH_CNTRL_DT
                if (BATCH_CNTRL_DT != null)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + BATCH_CNTRL_DT.ToString();			   
                }
                //BATCH_CNTRL_ACTV_IND
                if (BATCH_CNTRL_ACTV_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + BATCH_CNTRL_ACTV_IND.ToString();			   
                }
                //BATCH_START_DT
                if (BATCH_START_DT != null)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + BATCH_START_DT.ToString();			   
                }
                //BATCH_END_DT
                if (BATCH_END_DT != null)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + BATCH_END_DT.ToString();			   
                }
                //BOUNDARY_START_DT
                if (BOUNDARY_START_DT != null)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + BOUNDARY_START_DT.ToString();			   
                }
                //BOUNDARY_END_DT
                if (BOUNDARY_END_DT != null)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + BOUNDARY_END_DT.ToString();			   
                }
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE BATCH_CONTROL SET " + SET_CLAUSE.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + "UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        //PROCESS
        //This table stores the information about a Process
        public int DefineProcess(string PROCESS_ID_NM, int BATCH_ID, string PROCESS_TYP_CD, string PROCESS_LOCATION, string PROCESS_NM, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROCESS_ID_NM.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (BATCH_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (PROCESS_TYP_CD.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (PROCESS_LOCATION.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (PROCESS_NM.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    string insertTask = "INSERT INTO PROCESS (PROCESS_ID_NM, BATCH_ID, PROCESS_TYP_CD,  PROCESS_LOCATION, PROCESS_NM, ENABLE_IND, UPDATE_DTTM,UPDATE_UID) VALUES VALUES (@PROCESS_ID_NM, @BATCH_ID, @PROCESS_TYP_CD, @PROCESS_LOCATION, @PROCESS_NM, @ENABLE_IND, @UPDATE_DTTM, @UPDATE_UID)";
                    System.Data.SqlClient.SqlCommand insertTaskCmd = new System.Data.SqlClient.SqlCommand(insertTask, con);
                    insertTaskCmd.Parameters.Add("@PROCESS_ID_NM", SqlDbType.VarChar).Value = PROCESS_ID_NM;
                    insertTaskCmd.Parameters.Add("@BATCH_ID", SqlDbType.Int).Value = BATCH_ID;
                    insertTaskCmd.Parameters.Add("@PROCESS_TYP_CD", SqlDbType.VarChar).Value = PROCESS_TYP_CD;
                    insertTaskCmd.Parameters.Add("@PROCESS_LOCATION", SqlDbType.VarChar).Value = PROCESS_LOCATION;
                    insertTaskCmd.Parameters.Add("@PROCESS_NM", SqlDbType.VarChar).Value = PROCESS_NM;
                    insertTaskCmd.Parameters.Add("@ENABLE_IND", SqlDbType.Char).Value = 'Y';
                    insertTaskCmd.Parameters.Add("@UPDATE_DTTM", SqlDbType.DateTime2).Value = UPDATE_DTTM;
                    insertTaskCmd.Parameters.Add("@UPDATE_UID", SqlDbType.VarChar).Value = UPDATE_UID;
                    insertTaskCmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public Tuple<int, string, int, int, Tuple<string, string, string, string, DateTime, string>> GetProcess(int PROCESS_ID)
            {
            //Tuple<PROCESS_ID,PROCESS_ID_NM,BATCH_ID,Tuple< PROCESS_TYP_CD, PROCESS_LOCATION, PROCESS_NM, ENABLE_IND, UPDATE_DTTM, UPDATE_UUID>, ProcessStatus>
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if (PROCESS_ID <= 0)
                {
                ProcessStatus = 1;
                }

            if ((ProcessStatus == 0))
                {
                //Build Where Clause

                WHERE_CLAUSE = " PROCESS_ID =" + PROCESS_ID.ToString();


                //Build Query
                string queryCMD = "SELECT PROCESS_ID,PROCESS_ID_NM,BATCH_ID,PROCESS_TYP_CD, PROCESS_LOCATION, PROCESS_NM, ENABLE_IND, UPDATE_DTTM, UPDATE_UUID FROM PROCESS WHERE " + WHERE_CLAUSE.ToString();

                using (SqlConnection con = new SqlConnection(connectString))
                    {
                    using (SqlDataAdapter resultSet = new SqlDataAdapter(queryCMD, con))
                        {
                        resultSet.Fill(dtResult);
                        }
                    }

                //Build Return Tuple
                //Tuple<PROCESS_ID, PROCESS_ID_NM,BATCH_ID,Tuple< PROCESS_TYP_CD, PROCESS_LOCATION, PROCESS_NM, ENABLE_IND, UPDATE_DTTM, UPDATE_UUID>, ProcessStatus>

                if (dtResult.Rows.Count > 0)
                    {
                    DataRow rstRow = dtResult.Rows[0];
                    var rtrnTuple = new Tuple<int, string, int, int, Tuple<string, string, string, string, DateTime, string>>((int)rstRow["PROCESS_ID"], rstRow["PROCESS_ID_NM"].ToString(), (int)rstRow["BATCH_ID"], ProcessStatus, new Tuple<string, string, string, string, DateTime,string>(rstRow["PROCESS_TYP_CD"].ToString(), rstRow["PROCESS_LOCATION"].ToString(), rstRow["PROCESS_NM"].ToString(), rstRow["ENABLE_IND"].ToString(), (DateTime)rstRow["UPDATE_DTTM"], rstRow["UPDATE_UUID"].ToString()));
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus = 1;
                    var rtrnTuple = new Tuple<int, string, int, int, Tuple<string, string, string, string, DateTime, string>>(0, "", 0, ProcessStatus, new Tuple<string, string, string, string, DateTime, string>("", "", "", "", DateTime.Now, ""));
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus = 1;
                var rtrnTuple = new Tuple<int, string, int, int, Tuple<string, string, string, string, DateTime, string>>(0, "", 0, ProcessStatus, new Tuple<string, string, string, string, DateTime, string>("", "", "", "", DateTime.Now, ""));
                return rtrnTuple;
                }

            }

        public int EnableProcess(int PROCESS_ID, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'Y';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROCESS_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " PROCESS_ID=" + PROCESS_ID.ToString();

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE PROCESS SET ENABLE_IND=" + ENABLE_IND.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + ", UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DisableProcess(int PROCESS_ID, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'N';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROCESS_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " PROCESS_ID=" + PROCESS_ID.ToString();


                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE PROCESS SET ENABLE_IND=" + ENABLE_IND.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + ", UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int UpdateProcess(int PROCESS_ID, string PROCESS_ID_NM, int BATCH_ID, string PROCESS_TYP_CD, string PROCESS_LOCATION, string PROCESS_NM, string ENABLE_IND, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROCESS_ID <= 0)
                {
                ProcessStatus = 1;
                }

            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus = 1;
                }

            if (ProcessStatus == 0)
                {
                //Build Where Clause
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " PROCESS_ID=" + PROCESS_ID.ToString();

                //Build Set Clause
                string SET_CLAUSE = "";
                string SET_COMMA = "";
                int SetCounter = 0;

                //PROCESS_ID_NM
                if (PROCESS_ID_NM.Trim().Length > 0)
                    {
                    SET_CLAUSE = "PROCESS_ID_NM=" + PROCESS_ID_NM.ToString();
                    SetCounter++;
                    }
                //BATCH_ID
                if (BATCH_ID >= 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE + "BATCH_ID=" + BATCH_ID.ToString();
                    SetCounter++;
                    }
                //PROCESS_TYP_CD
                if (PROCESS_TYP_CD.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + PROCESS_TYP_CD.ToString();			   
                }
                //PROCESS_LOCATION
                if (PROCESS_LOCATION.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + PROCESS_LOCATION.ToString();			   
                }
                //PROCESS_NM
                if (PROCESS_NM.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + PROCESS_NM.ToString();			   
                }
                //ENABLE_IND
                if (ENABLE_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + ENABLE_IND.ToString();			   
                }

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE PROCESS SET " + SET_CLAUSE.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + "UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DeleteProcess(int PROCESS_ID)
            {
            int ProcessStatus = 0;
            if (PROCESS_ID <= 0)
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
        public int InsertAuditBalanceDefinition(int PROCESS_ID, string SOURCE_EXPRESSION_TXT, string OPERAND, string TARGET_EXPRESSION_TXT, string ABC_TYP_IND, string ABC_FAIL_CRITICAL_FLAG, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROCESS_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (SOURCE_EXPRESSION_TXT.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (OPERAND.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (TARGET_EXPRESSION_TXT.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ABC_TYP_IND.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ABC_FAIL_CRITICAL_FLAG.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    string insertTask = "INSERT INTO AUDIT_BALANCE_DEFINITION (PROCESS_ID, SOURCE_EXPRESSION_TXT, OPERAND,TARGET_EXPRESSION_TXT, ABC_TYP_IND, ENABLE_IND, ABC_FAIL_CRITICAL_FLAG,UPDATE_DTTM,UPDATE_UID) VALUES VALUES (@PROCESS_ID_NM, @SOURCE_EXPRESSION_TXT, @OPERAND, @TARGET_EXPRESSION_TXT, @ABC_TYP_IND, @ENABLE_IND, @ABC_FAIL_CRITICAL_FLAG, @UPDATE_DTTM, @UPDATE_UID)";
                    System.Data.SqlClient.SqlCommand insertTaskCmd = new SqlCommand(insertTask, con);
                    insertTaskCmd.Parameters.Add("@PROCESS_ID", SqlDbType.Int).Value = PROCESS_ID;
                    insertTaskCmd.Parameters.Add("@SOURCE_EXPRESSION_TXT", SqlDbType.VarChar).Value = SOURCE_EXPRESSION_TXT;
                    insertTaskCmd.Parameters.Add("@OPERAND", SqlDbType.VarChar).Value = OPERAND;
                    insertTaskCmd.Parameters.Add("@TARGET_EXPRESSION_TXT", SqlDbType.VarChar).Value = TARGET_EXPRESSION_TXT;
                    insertTaskCmd.Parameters.Add("@ABC_TYP_IND", SqlDbType.VarChar).Value = ABC_TYP_IND;
                    insertTaskCmd.Parameters.Add("@ENABLE_IND", SqlDbType.Char).Value = 'Y';
                    insertTaskCmd.Parameters.Add("@ABC_FAIL_CRITICAL_FLAG", SqlDbType.VarChar).Value = ABC_FAIL_CRITICAL_FLAG;
                    insertTaskCmd.Parameters.Add("@UPDATE_DTTM", SqlDbType.DateTime2).Value = UPDATE_DTTM;
                    insertTaskCmd.Parameters.Add("@UPDATE_UID", SqlDbType.VarChar).Value = UPDATE_UID;
                    insertTaskCmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public Tuple<int, int, DateTime, string, int, Tuple<string, string, string, string, string, string>> GetAuditBalanceDefinition(int PROCESS_ID, int ABC_RULE_ID)
            {
            //PROCESS_ID, ABC_RULE_ID, UPDATE_DTTM, UPDATE_UID, PROCESS_STATUS, inner tuple(SOURCE_EXPRESSION_TXT, OPERAND, TARGET_EXPRESSION_TXT, ABC_TYP_IND, ENABLE_IND, ABC_FAIL_CRITICAL_FLAG)
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if ((PROCESS_ID <= 0) && (ABC_RULE_ID <= 0))
                {
                ProcessStatus = 1;
                }

            if ((ProcessStatus == 0))
                {
                //Build Where Clause

                WHERE_CLAUSE = " PROCESS_ID =" + PROCESS_ID.ToString() + " ABC_RULE_ID =" + ABC_RULE_ID.ToString();


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
                    var rtrnTuple = new Tuple<int, int, DateTime, string, int, Tuple<string, string, string, string, string, string>>((int)rstRow["PROCESS_ID"], (int)rstRow["ABC_RULE_ID"], (DateTime)rstRow["UPDATE_DTTM"], rstRow["UPDATE_UID"].ToString(), ProcessStatus, new Tuple<string, string, string, string, string, string>(rstRow["SOURCE_EXPRESSION_TXT"].ToString(), rstRow["OPERAND"].ToString(), rstRow["TARGET_EXPRESSION_TXT"].ToString(), rstRow["ABC_TYP_IND"].ToString(), rstRow["ENABLE_IND"].ToString(), rstRow["ABC_FAIL_CRITICAL_FLAG"].ToString()));
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus = 1;
                    var rtrnTuple = new Tuple<int, int, DateTime, string, int, Tuple<string, string, string, string, string, string>>(0, 0, DateTime.Now, "", ProcessStatus, new Tuple<string, string, string, string, string, string>("", "", "", "", "", ""));
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus = 1;
                var rtrnTuple = new Tuple<int, int, DateTime, string, int, Tuple<string, string, string, string, string, string>>(0, 0, DateTime.Now, "", ProcessStatus, new Tuple<string, string, string, string, string, string>("", "", "", "", "", ""));
                return rtrnTuple;
                }

            }
        public int EnableAuditBalanceDefinition(int ABC_RULE_ID, int PROCESS_ID, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'Y';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (ABC_RULE_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (PROCESS_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " ABC_RULE_ID=" + ABC_RULE_ID.ToString() + " AND PROCESS_ID="+ PROCESS_ID.ToString();

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE AUDIT_BALANCE_DEFINITION SET ENABLE_IND=" + ENABLE_IND.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + ", UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DisableAuditBalanceDefinition(int ABC_RULE_ID, int PROCESS_ID, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'N';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (ABC_RULE_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " ABC_RULE_ID=" + ABC_RULE_ID.ToString() + " AND PROCESS_ID="+ PROCESS_ID.ToString();


                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE AUDIT_BALANCE_DEFINITION SET ENABLE_IND=" + ENABLE_IND.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + ", UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int UpdateAuditBalanceDefinition(int ABC_RULE_ID, int PROCESS_ID, string SOURCE_EXPRESSION_TXT, string OPERAND, string TARGET_EXPRESSION_TXT, string ABC_TYP_IND, string ENABLE_IND, string ABC_FAIL_CRITICAL_FLAG, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (ABC_RULE_ID <= 0)
                {
                ProcessStatus = 1;
                }

            if (PROCESS_ID <= 0)
                {
                ProcessStatus = 1;
                }

            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus = 1;
                }

            if (ProcessStatus == 0)
                {
                //Build Where Clause
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = "ABC_RULE_ID=" + ABC_RULE_ID.ToString() + " AND PROCESS_ID=" + PROCESS_ID.ToString();

                //Build Set Clause
                string SET_CLAUSE = "";
                string SET_COMMA = "";
                int SetCounter = 0;

                //SOURCE_EXPRESSION_TXT
                if (SOURCE_EXPRESSION_TXT.Trim().Length > 0)
                    {
                    SET_CLAUSE = "SOURCE_EXPRESSION_TXT=" + SOURCE_EXPRESSION_TXT.ToString();
                    SetCounter++;
                    }
                //OPERAND
                if (OPERAND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + OPERAND.ToString();
                    }
                //TARGET_EXPRESSION_TXT
                if (TARGET_EXPRESSION_TXT.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + TARGET_EXPRESSION_TXT.ToString();
                    }
                //ABC_TYP_IND
                if (ABC_TYP_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + ABC_TYP_IND.ToString();
                    }
                //ENABLE_IND
                if (ENABLE_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + ENABLE_IND.ToString();
                    }
                //ABC_FAIL_CRITICAL_FLAG
                if (ABC_FAIL_CRITICAL_FLAG.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + ABC_FAIL_CRITICAL_FLAG.ToString();
                    }

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE AUDIT_BALANCE_DEFINITION SET " + SET_CLAUSE.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + "UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DeleteAuditBalanceDefinition(int PROCESS_ID, int ABC_RULE_ID)
            {
            int ProcessStatus = 0;
            if ((PROCESS_ID <= 0) || (ABC_RULE_ID <= 0))
                {
                ProcessStatus++;
                }
            else
                {
                using (var dConn = new SqlConnection(connectString))
                using (var delCmd = dConn.CreateCommand())
                    {
                    dConn.Open();
                    delCmd.CommandText = "DELETE FROM AUDIT_BALANCE_DEFINITION WHERE PROCESS_ID = @PROCESS_ID AND ABC_RULE_ID=";
                    delCmd.Parameters.AddWithValue("@PROCESS_ID", PROCESS_ID.ToString());
                    delCmd.Parameters.AddWithValue("@ABC_RULE_ID", ABC_RULE_ID.ToString());
                    delCmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        //PROCESS_CONTROL
        //This table stores the information about each Process execution
        public int DefineProcessControl(int PROCESS_ID, int BATCH_CNTRL_ID, string PROCESS_CNTRL_STS, DateTime PROCESS_STR_DTTM, DateTime PROCESS_INIT_STR_DTTM)
            {
            int ProcessStatus = 0;
            Int64 PROCESS_RESTR_CNTR = 0;

            if (PROCESS_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (BATCH_CNTRL_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (PROCESS_CNTRL_STS.Length == 0)
                {
                ProcessStatus = 1;
                }

            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    string insertTask = "INSERT INTO PROCESS_CONTROL (BATCH_CNTRL_ID,PROCESS_ID, PROCESS_CNTRL_STS, PROCESS_STR_DTTM, PROCESS_RESTR_CNTR, PROCESS_INIT_STR_DTTM) VALUES VALUES (@BATCH_CNTRL_ID, @PROCESS_ID, @PROCESS_CNTRL_STS, @PROCESS_STR_DTTM, @PROCESS_RESTR_CNTR, @PROCESS_INIT_STR_DTTM)";
                    SqlCommand insertTaskCmd = new SqlCommand(insertTask, con);
                    insertTaskCmd.Parameters.Add("@BATCH_CNTRL_ID", SqlDbType.Int).Value = BATCH_CNTRL_ID;
                    insertTaskCmd.Parameters.Add("@PROCESS_ID", SqlDbType.Int).Value = PROCESS_ID;
                    insertTaskCmd.Parameters.Add("@PROCESS_CNTRL_STS", SqlDbType.VarChar).Value = PROCESS_CNTRL_STS;
                    insertTaskCmd.Parameters.Add("@PROCESS_STR_DTTM", SqlDbType.DateTime2).Value = PROCESS_STR_DTTM;
                    insertTaskCmd.Parameters.Add("@PROCESS_RESTR_CNTR", SqlDbType.BigInt).Value = PROCESS_RESTR_CNTR;
                    insertTaskCmd.Parameters.Add("@PROCESS_INIT_STR_DTTM", SqlDbType.DateTime2).Value = PROCESS_INIT_STR_DTTM;
                    insertTaskCmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public Tuple<int, int, Tuple<int, int, string, DateTime, DateTime, Int64, DateTime>> GetProcessControl(int PROCESS_CNTRL_ID, int ABC_RULE_ID)
            {
            //PROCESS_CNTRL_ID, ABC_RULE_ID, UPDATE_DTTM, UPDATE_UID, PROCESS_STATUS, inner tuple(SOURCE_EXPRESSION_TXT, OPERAND, TARGET_EXPRESSION_TXT, ABC_TYP_IND, ENABLE_IND, ABC_FAIL_CRITICAL_FLAG)
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if (PROCESS_CNTRL_ID <= 0)
                {
                ProcessStatus = 1;
                }

            if ((ProcessStatus == 0))
                {
                //Build Where Clause

                WHERE_CLAUSE = " PROCESS_CNTRL_ID =" + PROCESS_CNTRL_ID.ToString();


                //Build Query
                string queryCMD = "SELECT PROCESS_CNTRL_ID, BATCH_CNTRL_ID, PROCESS_ID, PROCESS_CNTRL_STS, PROCESS_STR_DDTM, PROCESS_END_DTTM, PROCESS_RESTR_CNTR, PROCESS_INIT_STR_DTTM FROM PROCESS_CONTROL WHERE " + WHERE_CLAUSE.ToString();

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
                    var rtrnTuple = new Tuple<int, int, Tuple<int, int, string, DateTime, DateTime, Int64, DateTime>>((int)rstRow["PROCESS_CNTRL_ID"], ProcessStatus, new Tuple<int, int, string, DateTime, DateTime, Int64, DateTime>((int)rstRow["BATCH_CNTRL_ID"], (int)rstRow["PROCESS_ID"], rstRow["PROCESS_CNTRL_STS"].ToString(), (DateTime)rstRow["PROCESS_STR_DDTM"], (DateTime)rstRow["PROCESS_END_DTTM"], (Int64)rstRow["PROCESS_RESTR_CNTR"], (DateTime)rstRow["PROCESS_INIT_STR_DTTM"]));
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus = 1;
                    var rtrnTuple = new Tuple<int, int, Tuple<int, int, string, DateTime, DateTime, Int64, DateTime>>(0, ProcessStatus, new Tuple<int, int, string, DateTime, DateTime, Int64, DateTime>(0, 0, "", DateTime.Now, DateTime.Now, 0,DateTime.Now));
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus = 1;
                var rtrnTuple = new Tuple<int, int, Tuple<int, int, string, DateTime, DateTime, Int64, DateTime>>(0, ProcessStatus, new Tuple<int, int, string, DateTime, DateTime, Int64, DateTime>(0, 0, "", DateTime.Now, DateTime.Now, 0, DateTime.Now));
                return rtrnTuple;
                }

            }

        public int UpdateProcessControl(int PROCESS_CNTRL_ID, int BATCH_CNTRL_ID, int PROCESS_ID, string PROCESS_CNTRL_STS, DateTime PROCESS_STR_DTTM, DateTime PROCESS_END_DTTM, Int64 PROCESS_RESTR_CNTR, DateTime PROCESS_INIT_STR_DTTM, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROCESS_CNTRL_ID <= 0)
                {
                ProcessStatus = 1;
                }

            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus = 1;
                }

            if (ProcessStatus == 0)
                {
                //Build Where Clause
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = "PROCESS_CNTRL_ID=" + PROCESS_CNTRL_ID.ToString();

                //Build Set Clause
                string SET_CLAUSE = "";
                string SET_COMMA = "";
                int SetCounter = 0;

                //PROCESS_CNTRL_STS
                if (PROCESS_CNTRL_STS.Trim().Length > 0)
                    {
                    SET_CLAUSE = "PROCESS_CNTRL_STS=" + PROCESS_CNTRL_STS.ToString();
                    SetCounter++;
                    }
                //BATCH_CNTRL_ID
                if (BATCH_CNTRL_ID > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + BATCH_CNTRL_ID.ToString();
                    }
                //PROCESS_ID
                if (PROCESS_ID > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + PROCESS_ID.ToString();
                    }

                //PROCESS_STR_DTTM
                if (PROCESS_STR_DTTM != null)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + PROCESS_STR_DTTM.ToString();
                    }
                //PROCESS_END_DTTM
                if (PROCESS_END_DTTM != null)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + PROCESS_END_DTTM.ToString();
                    }
                //PROCESS_RESTR_CNTR
                if (PROCESS_RESTR_CNTR > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + PROCESS_RESTR_CNTR.ToString();
                    }
                //PROCESS_INIT_STR_DTTM
                if (PROCESS_INIT_STR_DTTM != null)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + PROCESS_INIT_STR_DTTM.ToString();
                    }

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE PROCESS_CONTROL SET " + SET_CLAUSE.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + "UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        //PROCESS_PROPERTY
        //This table stores the properties for a Process that are entered during design time
        public int DefineProcessProperty(int PROCESS_ID, string PROPERTY_NM, string PROPERTY_VALUE, string PROPERTY_VALUE_TYP, string UPDATE_UID)
            {
            DateTime UPDATE_DTTM = DateTime.Now;
            int ProcessStatus = 0;

            if (PROCESS_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (PROPERTY_NM.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (PROPERTY_VALUE.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (PROPERTY_VALUE_TYP.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }

            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    string insertTask = "INSERT INTO PROCESS_PROPERTY (PROCESS_ID, PROPERTY_NM, PROPERTY_NM, PROPERTY_VALUE, PROPERTY_VALUE_TYP, ENABLE_IND, UPDATE_DTTM, UPDATE_UID) VALUES VALUES (@PROCESS_ID, @PROPERTY_NM, @PROPERTY_NM, @PROPERTY_VALUE, @PROPERTY_VALUE_TYP, @ENABLE_IND, @UPDATE_DTTM, @UPDATE_UID)";
                    SqlCommand insertTaskCmd = new SqlCommand(insertTask, con);
                    insertTaskCmd.Parameters.Add("@PROCESS_ID", SqlDbType.Int).Value = PROCESS_ID;
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

        public Tuple<int, string, int, Tuple<string, string, string, DateTime, string>> GetProcessProperty(int PROCESS_ID, string PROPERTY_NM)
            {
            //PROCESS_ID, PROPERTY_NM, PROPERTY_VALUE, PROPERTY_VALUE_TYP, ENABLED_IND, UPDATE_DTTM, UPDATE_UID
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if ((PROCESS_ID <= 0) || (PROPERTY_NM.Length == 0))
                {
                ProcessStatus = 1;
                }

            if ((ProcessStatus == 0))
                {
                //Build Where Clause

                WHERE_CLAUSE = " PROCESS_ID =" + PROCESS_ID.ToString() + " AND PROPERTY_NM LIKE '" + PROPERTY_NM.ToString().Trim() + "'";


                //Build Query
                string queryCMD = "SELECT PROCESS_ID, PROPERTY_NM, PROPERTY_VALUE, PROPERTY_VALUE_TYP, ENABLED_IND, UPDATE_DTTM, UPDATE_UID FROM PROCESS_PROPERTY WHERE " + WHERE_CLAUSE.ToString();

                using (SqlConnection con = new SqlConnection(connectString))
                    {
                    using (SqlDataAdapter resultSet = new SqlDataAdapter(queryCMD, con))
                        {
                        resultSet.Fill(dtResult);
                        }
                    }

                //Build Return Tuple
                //Tuple <PROCESS_ID,PROPERTY_NM,ProcessStatus, Tuple< PROPERTY_VALUE, PROPERTY_VALUE_TYP, ENABLED_IND, UPDATE_DTTM, UPDATE_UID>>

                if (dtResult.Rows.Count > 0)
                    {
                    DataRow rstRow = dtResult.Rows[0];
                    var rtrnTuple = new Tuple<int, string, int, Tuple<string, string, string, DateTime, string>>((int)rstRow["PROCESS_ID"], rstRow["PROPERTY_NM"].ToString(), ProcessStatus, new Tuple<string, string, string, DateTime, string>(rstRow["PROPERTY_VALUE"].ToString(), rstRow["PROPERTY_VALUE_TYP"].ToString(), rstRow["ENABLED_IND"].ToString(), (DateTime)rstRow["UPDATE_DTTM"], rstRow["UPDATE_UID"].ToString()));
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus = 1;
                    var rtrnTuple = new Tuple<int, string, int, Tuple<string, string, string, DateTime, string>>(0, "", ProcessStatus, new Tuple<string, string, string, DateTime, string>("", "", "", DateTime.Now, ""));
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus = 1;
                var rtrnTuple = new Tuple<int, string, int, Tuple<string, string, string, DateTime, string>> (0, "", ProcessStatus, new Tuple<string, string, string, DateTime, string>("", "", "", DateTime.Now, "")); ;
                return rtrnTuple;
                }

            }

        public int EnableProcessProperty(int PROCESS_ID, string PROPERTY_NM, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'Y';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROPERTY_NM.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (PROCESS_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " PROPERTY_NM=" + PROPERTY_NM.ToString() + " AND PROCESS_ID=" + PROCESS_ID.ToString();

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE PROCESS_PROPERTY SET ENABLE_IND=" + ENABLE_IND.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + ", UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DisableProcessProperty(int PROCESS_ID, string PROPERTY_NM, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            char ENABLE_IND = 'N';
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROPERTY_NM.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ProcessStatus == 0)
                {
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " PROPERTY_NM=" + PROPERTY_NM.ToString() + " AND PROCESS_ID=" + PROCESS_ID.ToString();


                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE PROCESS_PROPERTY SET ENABLE_IND=" + ENABLE_IND.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + ", UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int UpdateProcessProperty(int PROCESS_ID, string PROPERTY_NM, string PROPERTY_VALUE, string PROPERTY_VALUE_TYP, string ENABLE_IND, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROCESS_ID <= 0)
                {
                ProcessStatus = 1;
                }

            if (PROPERTY_NM.Trim().Length == 0)
                {
                ProcessStatus = 1;
                }

            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus = 1;
                }

            if (ProcessStatus == 0)
                {
                //Build Where Clause
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = "PROCESS_ID=" + PROCESS_ID.ToString() + ", PROPERTY_NM=" + PROPERTY_NM.ToString();

                //Build Set Clause
                string SET_CLAUSE = "";
                string SET_COMMA = "";
                int SetCounter = 0;

                //PROPERTY_VALUE
                if (PROPERTY_VALUE.Trim().Length > 0)
                    {
                    SET_CLAUSE = "PROPERTY_VALUE=" + PROPERTY_VALUE.ToString();
                    SetCounter++;
                    }

                //PROPERTY_VALUE_TYP
                if (PROPERTY_VALUE_TYP.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE =SET_COMMA.ToString()+ "PROPERTY_VALUE_TYP=" + PROPERTY_VALUE_TYP.ToString();
                    }

                //ENABLE_IND
                if (ENABLE_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_COMMA.ToString() + "ENABLE_IND=" + ENABLE_IND.ToString();
                    }

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE PROCESS_PROPERTY SET " + SET_CLAUSE.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + "UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public int DeleteProcessProperty(int PROCESS_ID, string PROPERTY_NM)
            {
            int ProcessStatus = 0;
            if ((PROCESS_ID <= 0) || (PROPERTY_NM.Trim().Length == 0))
                {
                ProcessStatus++;
                }
            else
                {
                using (var dConn = new SqlConnection(connectString))
                using (var delCmd = dConn.CreateCommand())
                    {
                    dConn.Open();
                    delCmd.CommandText = "DELETE FROM AUDIT_BALANCE_DEFINITION WHERE PROCESS_ID = @PROCESS_ID AND PROPERTY_NM=";
                    delCmd.Parameters.AddWithValue("@PROCESS_ID", PROCESS_ID.ToString());
                    delCmd.Parameters.AddWithValue("@PROPERTY_NM", PROPERTY_NM.ToString());
                    delCmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        //PROCESS_CONTROL_STATS
        //This table stores the metadata of the execution as passed from the Process
        public int DefineProcessControlStats(int PROCESS_CNTRL_ID, string PROCESS_CNTRL_STAT_NM, string PROCESS_CNTRL_STAT_DESC, string PROCESS_CNTRL_STAT_TYP, string PROCESS_CNTRL_STAT_VALU, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;
            if (PROCESS_CNTRL_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (PROCESS_CNTRL_STAT_NM.Trim().Length == 0)
                {
                ProcessStatus = 1;
                }

            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    string insertTask = "INSERT INTO PROCESS_CONTROL_STATS (PROCESS_CNTRL_ID, PROCESS_CNTRL_STAT_NM , PROCESS_CNTRL_STAT_DESC, PROCESS_CNTRL_STAT_TYP,PROCESS_CNTRL_STAT_VALU, UPDATE_DTTM, UPDATE_UID) VALUES VALUES (@PROCESS_CNTRL_ID, @PROCESS_CNTRL_STAT_NM, @PROCESS_CNTRL_STAT_DESC, @PROCESS_CNTRL_STAT_TYP, @PROCESS_CNTRL_STAT_VALU, @UPDATE_DTTM,@UPDATE_UID)";
                    SqlCommand insertTaskCmd = new SqlCommand(insertTask, con);
                    insertTaskCmd.Parameters.Add("@PROCESS_CNTRL_ID", SqlDbType.Int).Value = PROCESS_CNTRL_ID;
                    insertTaskCmd.Parameters.Add("@PROCESS_CNTRL_STAT_NM", SqlDbType.VarChar).Value = PROCESS_CNTRL_STAT_NM;
                    insertTaskCmd.Parameters.Add("@PROCESS_CNTRL_STAT_DESC", SqlDbType.VarChar).Value = PROCESS_CNTRL_STAT_DESC;
                    insertTaskCmd.Parameters.Add("@PROCESS_CNTRL_STAT_TYP", SqlDbType.DateTime2).Value = PROCESS_CNTRL_STAT_TYP;
                    insertTaskCmd.Parameters.Add("@PROCESS_CNTRL_STAT_VALU", SqlDbType.VarChar).Value = PROCESS_CNTRL_STAT_VALU;
                    insertTaskCmd.Parameters.Add("@UPDATE_DTTM", SqlDbType.DateTime2).Value = UPDATE_DTTM;
                    insertTaskCmd.Parameters.Add("@UPDATE_UID", SqlDbType.VarChar).Value = UPDATE_UID;
                    insertTaskCmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        public Tuple<int, string, string, string, string, DateTime, string, int> GetProcessControlStats(int PROCESS_CNTRL_ID, string PROCESS_CNTRL_STAT_NM)
            {
            //int PROCESS_CNTRL_ID, string PROCESS_CNTRL_STAT_NM , string PROCESS_CNTRL_STAT_DESC, string PROCESS_CNTRL_STAT_TYP, string PROCESS_CNTRL_STAT_VALU ,string UPDATE_UID
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if (PROCESS_CNTRL_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (PROCESS_CNTRL_STAT_NM.Trim().Length == 0)
                {
                ProcessStatus = 1;
                }

            if ((ProcessStatus == 0))
                {
                //Build Where Clause

                WHERE_CLAUSE = " PROCESS_CNTRL_ID =" + PROCESS_CNTRL_ID.ToString() + " AND PROCESS_CNTRL_STAT_NM=" + PROCESS_CNTRL_STAT_NM.ToString();


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
                    var rtrnTuple = new Tuple<int, string, string, string, string, DateTime, string, int>((int)rstRow["PROCESS_CNTRL_ID"], rstRow["PROCESS_CNTRL_STAT_DESC"].ToString(), rstRow["PROCESS_CNTRL_STAT_NM"].ToString(), rstRow["PROCESS_CNTRL_STAT_TYP"].ToString(), rstRow["PROCESS_CNTRL_STAT_VALU"].ToString(), (DateTime)rstRow["UPDATE_DTTM"], rstRow["UPDATE_UID"].ToString(), ProcessStatus);
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus = 1;
                    var rtrnTuple = new Tuple<int, string, string, string, string, DateTime, string, int>(0, "", "", "", "", DateTime.Now, "", ProcessStatus);
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus = 1;
                var rtrnTuple = new Tuple<int, string, string, string, string, DateTime, string, int>(0, "", "", "", "", DateTime.Now, "", ProcessStatus);
                return rtrnTuple;
                }
            }

        public int UpdateProcessControlStats(int PROCESS_CNTRL_ID, string PROCESS_CNTRL_STAT_NM, string PROCESS_CNTRL_STAT_DESC, string PROCESS_CNTRL_STAT_TYP, string PROCESS_CNTRL_STAT_VALU, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROCESS_CNTRL_ID <= 0)
                {
                ProcessStatus = 1;
                }

            if (PROCESS_CNTRL_STAT_NM.Trim().Length == 0)
                {
                ProcessStatus = 1;
                }

            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus = 1;
                }

            if (ProcessStatus == 0)
                {
                //Build Where Clause
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = " PROCESS_CNTRL_ID =" + PROCESS_CNTRL_ID.ToString() + " AND PROCESS_CNTRL_STAT_NM=" + PROCESS_CNTRL_STAT_NM.ToString();

                //Build Set Clause
                string SET_CLAUSE = "";
                string SET_COMMA = "";
                int SetCounter = 0;

                //PROCESS_CNTRL_STAT_DESC
                if (PROCESS_CNTRL_STAT_DESC.Trim().Length > 0)
                    {
                    SET_CLAUSE = "PROCESS_CNTRL_STAT_DESC=" + PROCESS_CNTRL_STAT_DESC.ToString();
                    SetCounter++;
                    }
                //PROCESS_CNTRL_STAT_TYP
                if (PROCESS_CNTRL_STAT_TYP.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + PROCESS_CNTRL_STAT_TYP.ToString();
                    }
                //PROCESS_CNTRL_STAT_VALU
                if (PROCESS_CNTRL_STAT_VALU.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_CLAUSE.ToString() + SET_COMMA.ToString() + PROCESS_CNTRL_STAT_VALU.ToString();
                    }


                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE PROCESS_CONTROL_STATS SET " + SET_CLAUSE.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + "UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
                    cmd.Connection = con;
                    cmd.ExecuteNonQuery();
                    }
                }
            return ProcessStatus;
            }

        //PROCESS_ABC
        //This table stores the results of the Audit Balance and Control measuremnts for each Process for each run
        public int StoreProcessABCResults(int PROCESS_ID, int PROCESS_CNTRL_ID, int ABC_RULE_ID, string EXPECTED_VALUE, string ACTUAL_VALUE, string ABC_PASS_IND, string PROCESS_BALANCE_IND, string PROCESS_CONTROL_IND, string PROCESS_AUDIT_IND, string CRITICAL_FAIL_IND, string UPDATE_UID)
            {
            DateTime UPDATE_DTTM = DateTime.Now;
            int ProcessStatus = 0;

            if (PROCESS_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (PROCESS_CNTRL_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (ABC_RULE_ID <= 0)
                {
                ProcessStatus = 1;
                }
            if (EXPECTED_VALUE.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ACTUAL_VALUE.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (ABC_PASS_IND.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (PROCESS_BALANCE_IND.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (PROCESS_CONTROL_IND.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (PROCESS_AUDIT_IND.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (CRITICAL_FAIL_IND.Length == 0)
                {
                ProcessStatus = 1;
                }
            if (UPDATE_UID.Length == 0)
                {
                ProcessStatus = 1;
                }

            if (ProcessStatus == 0)
                {
                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    string insertTask = "INSERT INTO PROCESS_ABC (PROCESS_ID, PROCESS_CNTRL_ID, ABC_RULE_ID,EXPECTED_VALUE, ACTUAL_VALUE,  ABC_PASS_IND, PROCESS_BALANCE_IND, PROCESS_CONTROL_IND, PROCESS_AUDIT_IND, CRITICAL_FAIL_IND, UPDATE_DTTM, UPDATE_UID) VALUES (@PROCESS_ID, @PROCESS_CNTRL_ID, @ABC_RULE_ID,EXPECTED_VALUE, @ACTUAL_VALUE, @ ABC_PASS_IND, @PROCESS_BALANCE_IND, @PROCESS_CONTROL_IND, @PROCESS_AUDIT_IND, @CRITICAL_FAIL_IND, @UPDATE_DTTM, @UPDATE_UID)";
                    SqlCommand insertTaskCmd = new SqlCommand(insertTask, con);
                    insertTaskCmd.Parameters.Add("@PROCESS_ID", SqlDbType.Int).Value = PROCESS_ID;
                    insertTaskCmd.Parameters.Add("@PROCESS_CNTRL_ID", SqlDbType.Int).Value = PROCESS_CNTRL_ID;
                    insertTaskCmd.Parameters.Add("@ABC_RULE_ID", SqlDbType.Int).Value = ABC_RULE_ID;
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

        public Tuple<int, int, int, DateTime, string, int, Tuple<string, string, string, string, string, string>> GetProcessABCResult(int PROCESS_ID, int PROCESS_CNTRL_ID, int ABC_RULE_ID)
            {
            //PROCESS_ID, PROCESS_CNTRL_ID, ABC_RULE_ID, EXPECTED_VALUE, ACTUAL_VALUE, ABC_PASS_IND, PROCESS_BALANCE_IND, PROCESS_AUDIT_IND, CRITICAL_FAIL_IND, UPDATE_DTTM, UPDATE_UID
            int ProcessStatus = 0;
            string WHERE_CLAUSE = "";
            DataTable dtResult = new DataTable();

            if ((PROCESS_ID <= 0) || (PROCESS_CNTRL_ID <= 0) || (ABC_RULE_ID <= 0))
                {
                ProcessStatus = 1;
                }

            if ((ProcessStatus == 0))
                {
                //Build Where Clause

                WHERE_CLAUSE = " PROCESS_ID =" + PROCESS_ID.ToString() + " AND PROCESS_CNTRL_ID =" + PROCESS_CNTRL_ID.ToString() + "AND ABC_RULE_ID =" + ABC_RULE_ID.ToString(); ;


                //Build Query
                string queryCMD = "SELECT PROCESS_ID, PROCESS_CNTRL_ID, ABC_RULE_ID, EXPECTED_VALUE, ACTUAL_VALUE, ABC_PASS_IND, PROCESS_BALANCE_IND, PROCESS_AUDIT_IND, CRITICAL_FAIL_IND, UPDATE_DTTM, UPDATE_UIDD FROM PROCESS_ABC WHERE " + WHERE_CLAUSE.ToString();

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
                    var rtrnTuple = new Tuple<int, int, int, DateTime, string, int, Tuple<string, string, string, string, string, string>>((int)rstRow["PROCESS_ID"], (int)rstRow["PROCESS_CNTRL_ID"], (int)rstRow["ABC_RULE_ID"], (DateTime)rstRow["UPDATE_DTTM"], rstRow["UPDATE_UID"].ToString(), ProcessStatus, new Tuple<string, string, string, string, string, string>(rstRow["EXPECTED_VALUE"].ToString(), rstRow["ACTUAL_VALUE"].ToString(), rstRow["ABC_PASS_IND"].ToString(), rstRow["PROCESS_BALANCE_IND"].ToString(), rstRow["PROCESS_AUDIT_IND"].ToString(), rstRow["CRITICAL_FAIL_IND"].ToString()));
                    return rtrnTuple;
                    }
                else
                    {
                    //Build empty Tuple
                    ProcessStatus = 1;
                    var rtrnTuple = new Tuple<int, int, int, DateTime, string, int, Tuple<string, string, string, string, string, string>>(0, 0, ProcessStatus, DateTime.Now, "", 0, new Tuple<string, string, string, string, string, string>("", "", "", "", "", ""));
                    return rtrnTuple;
                    }
                }
            else
                {
                //Build empty Tuple
                ProcessStatus = 1;
                var rtrnTuple = new Tuple<int, int, int, DateTime, string, int, Tuple<string, string, string, string, string, string>>(0, 0, ProcessStatus, DateTime.Now, "", 0, new Tuple<string, string, string, string, string, string>("", "", "", "", "", ""));
                return rtrnTuple;
                }

            }

        public int UpdateProcessABCResult(int PROCESS_ID, int PROCESS_CNTRL_ID, int ABC_RULE_ID, string EXPECTED_VALUE, string ACTUAL_VALUE, string ABC_PASS_IND, string PROCESS_BALANCE_IND, string PROCESS_CONTROL_IND, string PROCESS_AUDIT_IND, string CRITICAL_FAIL_IND, string UPDATE_UID)
            {
            int ProcessStatus = 0;
            DateTime UPDATE_DTTM = DateTime.Now;

            if (PROCESS_ID <= 0)
                {
                ProcessStatus = 1;
                }

            if (PROCESS_CNTRL_ID <= 0)
                {
                ProcessStatus = 1;
                }

            if (ABC_RULE_ID <= 0)
                {
                ProcessStatus = 1;
                }

            if (UPDATE_UID.Trim().Length == 0)
                {
                ProcessStatus = 1;
                }

            if (ProcessStatus == 0)
                {
                //Build Where Clause
                string WHERE_CLAUSE = "";
                WHERE_CLAUSE = "PROCESS_ID=" + PROCESS_ID.ToString() + "AND PROCESS_CNTRL_ID=" + PROCESS_CNTRL_ID.ToString() + " AND ABC_RULE_ID=" + ABC_RULE_ID.ToString();

                //Build Set Clause
                string SET_CLAUSE = "";
                string SET_COMMA = "";
                int SetCounter = 0;

                //EXPECTED_VALUE
                if (EXPECTED_VALUE.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = "EXPECTED_VALUE=" + EXPECTED_VALUE.ToString();
                    }

                //ACTUAL_VALUE
                if (ACTUAL_VALUE.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_COMMA.ToString() + "ACTUAL_VALUE=" + ACTUAL_VALUE.ToString();
                    SetCounter++;
                    }

                //ABC_PASS_IND
                if (ABC_PASS_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_COMMA.ToString() + "ABC_PASS_IND=" + ABC_PASS_IND.ToString();
                    }

                //PROCESS_BALANCE_IND
                if (PROCESS_BALANCE_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_COMMA.ToString() + "PROCESS_BALANCE_IND=" + PROCESS_BALANCE_IND.ToString();
                    }

                //PROCESS_CONTROL_IND
                if (PROCESS_CONTROL_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_COMMA.ToString() + "PROCESS_CONTROL_IND=" + PROCESS_CONTROL_IND.ToString();
                    }

                //PROCESS_AUDIT_IND
                if (PROCESS_AUDIT_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_COMMA.ToString() + "PROCESS_AUDIT_IND=" + PROCESS_AUDIT_IND.ToString();
                    }

                //CRITICAL_FAIL_IND
                if (CRITICAL_FAIL_IND.Trim().Length > 0)
                    {
                    if (SetCounter > 0) { SET_COMMA = ", "; } else { SetCounter++; }
                    SET_CLAUSE = SET_COMMA.ToString() + "CRITICAL_FAIL_IND=" + CRITICAL_FAIL_IND.ToString();
                    }

                using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(connectString))
                    {
                    con.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "UPDATE PROCESS_ABC SET " + SET_CLAUSE.ToString() + ", UPDATE_UUID=" + UPDATE_UID.ToString() + "UPDATE_DTTM=" + UPDATE_DTTM.ToString() + " WHERE " + WHERE_CLAUSE.ToString();
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
