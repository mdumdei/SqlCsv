<#
.SYNOPSIS
    Uploads a CSV file as a table parameter to an SQL server.
.DESCRIPTION
    This script reads a CSV file, converts it to a table, and sends it to the SQL server for processing. There are 2 scenarios for this script:
      1. Uploading a CSV to a SQL table
      2. Uploading a CSV file for custom processing by a user defined stored procedure.
    
    Use the -CreateXXXXXDDL options to generate template code for configuring pre-requisites needed before actually sending the CSV data.
.PARAMETER Upload
    Run script in 'Upload CSV to a table' mode (default).
.PARAMETER Process
    Run script in 'Upload CSV to a user-defined process' mode.
.PARAMETER CreateUploadDDL
    Run script in 'Create an SQL template for configuring pre-requisites for uploading CSV data to a table' mode.
.PARAMETER CreateProcessDDL
    Run script in 'Create an SQL template for configuring pre-requisites for uploading CSV data for use in a custom process' mode.
.PARAMETER CsvFile
    The path to the CSV file. The 'create configuration template' modes use this to define a table type based on the data in the CSV. For normal processing, this will be the data sent to the server.
.PARAMETER Server
    The SQL Server to connect to. If you always use the same server, you can hard code the name - see the top of the script.
.PARAMETER Database
    The database to connect to. The database you want to use on the server. This may be hard coded if you always are using the same database.
.PARAMETER Credential
    The credential to use to connect to the SQL Server. Default is the current user.
.PARAMETER ProcedureName
    The stored procedure that will process the uploaded CSV. When using the -CreateXXXXX switches, code will be generated in the setup SQL to create this stored procedure - it does not need to and should not already exist. In normal mode, after running the SQL setup DDL, this is the procedure that will process the uploaded CSV data.
.PARAMETER TableName
    The SQL table to send the data to. It does not have to exist as long as the user who created the stored procedure has permissions to create tables.
.PARAMETER TableTypeName
    A name to assign to the table type when using the -CreateUploadDDL or -CreateProcessDDL switches. This does not actually create the TableType but defines a type with this name in the SQL generated to configure pre-requisites.
.PARAMETER Create
    Applies when uploading a CSV to a database table. Delete and re-create the table before sending the data.
.PARAMETER Truncate
    Applies when uploading a CSV to a database table. Truncate the table before sending the data.
.PARAMETER ReturnRows
    Used with 'Process' CSV mode invocations and specifies the process not only uploads the CSV for processing but returns a dataset.
.PARAMETER AuthorizedUser
    The user, role, or other security object that is allowed to EXEC the stored procedure and granted access to the TableType.
.EXAMPLE
    PS:\>SqlCsv.ps1 -CsvFile Xyz.csv -ProcedureName spUploadXyz -Server mySqlServer -Database dbProd
    Upload Xyz.csv to a table. The table will be created if it does not already exist. If there are existing records, the new CSV input will be appended.
    Requires pre-configuration setup using the -CreateUploadDDL switch. The DDL template produced during that process embeds the target table for the upload within the generated stored procedure.
.EXAMPLE
    PS:\>SqlCsv.ps1 -CsvFile Xyz.csv -ProcedureName spUploadXyz -Truncate -Server mySqlServer -Database dbProd
    Upload Xyz.csv to a table named J1XyzTbl in the dbProd database. Existing table records are deleted before the upload.
    Requires pre-configuration setup using the -CreateUploadDDL switch. The DDL template produced during that process embeds the target table for the upload within the generated stored procedure.
.EXAMPLE
    PS:\>SqlCsv.ps1 -Process -CsvFile abc.csv -ProcedureName spCustomAbcProcess -Server mySqlServer -Database dbProd
    Upload abc.csv to a user defined stored procedure. The stored procedure can use the uploaded CSV in JOINs as though it were a native table or a table variable for any type of SQL operation: SELECT, INSERT, DELETE, UPDATE, ... 
    Requires pre-configuration setup using the -CreateProcessDDL switch.
.EXAMPLE
    PS:\>SqlCsv.ps1 -CreateUploadDDL -CsvFile xyz.csv -ProcedureName spUploadXyz -TableName myTable -TableTypeName ttXyzType -AuthorizedUser XyzUser 
    Create an SQL code snippet to configure uploading data files that match the column data in xyz.csv. The snippet will contain a definition for the TableType, a definition for the SQL procedure that loads the table, and grant necessary permissions for the end user.
.EXAMPLE
    PS:\>SqlCsv.ps1 -CreateProcessDDL -CsvFile abc.csv -ProcedureName spCustomAbcProcess -AuthorizedUser AbcUser -TableTypeName ttAbcType
    Create an SQL code snippet to configure uploading data files that match the column data in abc.csv. The snippet will contain a definition for the TableType, a shell definition for a custom stored procedure that will make use of the uploaded CSV, and grant necessary permissions for the end user.
.NOTES
    Mike Dumdei, TC3
#>
using namespace System.Data
using namespace System.Data.SqlClient
using namespace System.Collections.Generic

[CmdletBinding(DefaultParameterSetName='Upload')]
param (
    [Parameter(ParameterSetName='Upload')][switch]$Upload,
    [Parameter(ParameterSetName='Process', Mandatory)][switch]$Process,
    [Parameter(ParameterSetName='DDLUpload', Mandatory)][switch]$CreateUploadDDL,
    [Parameter(ParameterSetName='DDLProcess', Mandatory)][switch]$CreateProcessDDL,
    [Parameter(ParameterSetName='DDLUpload', Mandatory)]
    [Parameter(ParameterSetName='DDLProcess', Mandatory)]
    [Parameter(ParameterSetName='Upload', Mandatory)]
    [Parameter(ParameterSetName='Process',Mandatory=$true)][string]$CsvFile,
    [Parameter(ParameterSetName='DDLUpload', Mandatory)]
    [Parameter(ParameterSetName='DDLProcess', Mandatory)]
    [Parameter(ParameterSetName='Upload', Mandatory)]
    [Parameter(ParameterSetName='Process',Mandatory=$true)][string]$ProcedureName,
    [Parameter(ParameterSetName='DDLUpload', Mandatory)]
    [Parameter(ParameterSetName='DDLProcess', Mandatory)]$AuthorizedUser,
    [Parameter(ParameterSetName='DDLUpload', Mandatory)]
    [Parameter(ParameterSetName='DDLProcess', Mandatory)][string]$TableTypeName,
    [Parameter(ParameterSetName='DDLUpload', Mandatory)][string]$TableName,
    [Parameter(ParameterSetName='Upload')][switch]$Create,
    [Parameter(ParameterSetName='Upload')][switch]$Truncate,
    [Parameter(ParameterSetName='Process')][switch]$ReturnRows,
    # You may want to hard code $Server and $Database values, remove Mandatory if you do
    # or set one or both as variables following the parameter list and don't include in 
    # the parameter list at all.
    # ... $Server = 'mySQLServer',
    # ... $Database = 'targetDBName',
    [Parameter(ParameterSetName='Upload', Mandatory)]
    [Parameter(ParameterSetName='Process', Mandatory)][string]$Server,
    [Parameter(ParameterSetName='Upload', Mandatory)]
    [Parameter(ParameterSetName='Process', Mandatory)][string]$Database,
    # If no credential supplied, defaults to Integrate Security (run as current user)
    [Parameter(ParameterSetName='Upload')]
    [Parameter(ParameterSetName='Process')][PSCredential]$Credential
)

function Get-DDL {
    $csv = Import-Csv $CsvFile
    $tbl = Convert-ListToDataTable $csv
    $ddl = Get-TvpTypeDDL -TvpName $TableTypeName -Table $tbl -WithDrop -szVarchar 512 -szDecimal '10,2'
    return $ddl
}

function Open-SqlConnection {
    <#
    .SYNOPSIS
        Open a connection to a SQL server.
    .DESCRIPTION
        Open an SQL connection to a server. Invoke-SqlQuery automatically opens and closes connections using 1) the ConnectionString parameter, or 2) the Server and SqlDatabase parameters. Calling Open-SqlConnection directly is only necessary if you want to pass Invoke-SqlQuery an open connection via the -Connection parameter and need to get a SqlConnection object. All parameters may be stored in the module level $Global:SFG.SQL hashtable vs. passing them as parameters. Directly passed parameters override any stored in $Global:SFG.SQL.
    .PARAMETER ConnectionString
        Connection string to use for the connection. Credential may be embedded or passed in the Credential parameter- don't do both. If the -IntegratedSecurity switch is set, any embedded credential will be removed and Integrated Security will be added to the connection string.
        Aliases: ConnStr, SqlConnStr  SFGKey: $Global:SFG.Sql.ConnectionString
        NOTE: Integrated Security will NOT be automatically added if using a connection string unless the -IntegratratedSecurity switch is used.
    .PARAMETER Credential
        Credential for connection - overridden by -IntegratedSecurity switch if both present. Do not embed the credential in the connection string if you are passing it separately.
        Aliases: Cred, SqlCred       SFGKey: $Global:SFG.Sql.Credential
    .PARAMETER IntegratedSecurity
        Use IntegratedSecurity -- even if a Credential is present. This is automatically assigned if using a Server/Database and no Credential.
        Aliases: TrustedConnection   SFGKey: $Global:SFG.Sql.IntegratedSecurity
    .PARAMETER Server
        If not using a connection string, this is the server for the connection.
        Aliases: SqlSrv              SFGKey: $Global:SFG.Sql.Server
    .PARAMETER Database
        If not using a connection string, this is the database for the connection.
        Aliases: SqlDB               SFGKey: $Global:SFG.Sql.Database
    .EXAMPLE
        PS:\>$connStr = "Server=$srv1;Database=$db;"
        PS:\>$conn = Open-SqlConnection -ConnectionString $connStr -Credential $Cred

        Open a SQL connection using a connection string. Credentials are passed separately. You can also embed them in the connection string. 
    .EXAMPLE
        PS:\>$conn = Open-SqlConnection -Server Srv1 -DataBase DB1 -Credential $Cred

        Open an SQL connection to Srv1 with the default database set to DB1.
    .EXAMPLE
        PS:\>$connStr = "Server=$srv1;Database=$db;MultipleActiveResultSets=true;User ID=$user;Password=$pass;"
        PS:\>$conn = Open-SqlConnection -ConnectionString $connStr

        Open an SQL connection using a connection string and a plaintext password stored in a PS variable.
    .NOTES
        Author: Mike Dumdei
    #>
    [OutputType([System.Data.SqlClient.SQLConnection])]
    [CmdletBinding(DefaultParameterSetName='SrvDB')]
    param (
        [Parameter(ParameterSetName='ConnStr')][Alias('SqlConnStr','ConnStr')][string]$ConnectionString,
        [Parameter(ParameterSetName='SrvDB')][Alias('SqlSrv')][string]$Server,
        [Parameter(ParameterSetName='SrvDB')][Alias('SqlDB')][string]$DataBase,
        [Parameter(ParameterSetName='ConnStr')]
        [Parameter(ParameterSetName='SrvDB')]
        [Parameter()][Alias('SqlCred','Cred')][Object]$Credential,
        [Parameter()][Alias('TrustedConnection')][switch]$IntegratedSecurity
    )
    try {
        if (!$ConnectionString) {
            if (!$Server -or !$Database) {
                throw "Connection, ConnectionString, or Server/Database must be provided"
            }
            $ConnectionString = "Data Source=$Server;Initial Catalog=$Database;"
            if (!$Credential) {
                $IntegratedSecurity = $true
            }
        }
        if ($IntegratedSecurity) {
            if ($ConnectionString -notmatch '(Integrated|Trusted)[^;]*=\s*[^;]*\s*;?\s*') {
                $ConnectionString = $ConnectionString -replace '(User|Uid|Pass|Pwd)[^;]*=\s*[^;]*\s*;?\s*', ''
                $ConnectionString = "$($ConnectionString.TrimEnd(';'));Integrated Security=true;"
            }            
            $Connection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString)
        } elseif ($Credential) {
            $ConnectionString = $ConnectionString -replace '(Integrated|Trusted)[^;]*=\s*[^;]*\s*;?\s*'
            if ($Credential -is [PSCredential]) {
                $Credential.Password.MakeReadOnly()
                $Credential = New-Object System.Data.SqlClient.SqlCredential($Credential.UserName, $Credential.Password)
                $Connection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString, $Credential)
            } else {
                throw "Invalid credential type: $($Credential.GetType().Name)"
            }
        }
        $Connection.Open()
    } catch {
        $Connection = $null
        $err = $_
    }
    if (!$Connection -or $Connection.State -ne 'Open') {
        if ($Credential) {
            $Credential = "[User ID=$($Credential.UserId),Password='*pass*']"
        }
        $debug = @{
            ConnectionString = $ConnectionString
            Server = $Server
            Database = $Database
            Credential = $Credential
            IntegratedSecurity = $IntegratedSecurity
        }
        Write-Host -ForegroundColor Red "Open-SqlConnection failed"
        $debug
    }
    return $Connection
}


function Invoke-SqlQuery {
    <#
    .SYNOPSIS
        Execute a Reader, Scalar, or NonQuery SQL query with optional capture to a trace log. Aliased as RunSql.
    .DESCRIPTION
        The purpose of the Invoke-SqlQuery command is 1) to centralize SQL calls a script makes to a single function, and 2) add the ability to trace SQL commands and query results obtained during execution of the script. Invoke-SqlQuery processes all 3 of the basic query types: Reader, Scalar, and NonQuery. Reader queries are implemented as SQL ExecuteSqlReader calls, Scalars as ExecuteScalar, and NonQuerys as ExecuteNonQuery.

        Invoke-SqlQuery supports paramertized queries, table value parameter queries, and both text and stored procedure query requests. To run a stored procedure, begin the query text with 'EXEC' followed by a space. To add parameters to a SQL query, use standard @arg1, @val notation in the Query text followed by a -Parameters @{ arg1 = 'Sales'; val = 'Qtr1' } Invoke-SqlQuery parameter to specify the values.

        -- Tracing --
        Setting the $Global:SqlDebug or $SFG.SqlDebug to $true activates an in-memory trace of each query processed by Invoke-SqlQuery. Trace items are PSCustomObjects that contain the server (Srv) and database (DB) accessed, the query text (Cmd), query parameters (Parms), and the resulting output (Data). Trace information can be accessed as objects ($Global:SqlHist) or as string items suitable for viewing on the console or writing to a text file using Write-SqlHist. 

        -- Connections --
        SQL connections can be passed as either an already open SQLConnection object, a ConnectionString, or as a the SQL server and database you want to connect to. 

        -- Credentials --
        Credentials can be embedded in the connection string or passed separately as an argument. If no credential is supplied the connection is created as the logged in user.
        
        -- Parameter Presets --
        Parameter values related to the connection may be preset in the SFG module $Global:SFG hashtable. Doing so, simplifies calls since you no longer need to pass connection strings, credentials, etc. Any parameters directly specified will override existing $Global:SFG values. See Example 2.
    .PARAMETER Reader
        Switch parameter identifying the query returns tabular results. This is the default if Scalar or NonQuery are not specified.
    .PARAMETER Scalar
        Switch parameter identifying the query returns a single data value.
    .PARAMETER NonQuery
        Switch parameter identifying the query does not return values from the database. Use for INSERT, UPDATE, DELETE statements. Returns number of rows affected.
    .PARAMETER Query
        The query string for the query. Precede the 'EXEC ' or 'EXECUTE ' to run a stored procedure.
        Aliases: QueryStr, SqlQuery
    .PARAMETER Parameters
        Parameter table if using parameterized queries or a stored procedure. Pass as key/value pairs (hashtable).
        Aliases: Params, SqlParams
    .PARAMETER TimeOut
        Time in seconds before the query times out (command timeout). Use for long running queries.
        Aliases: CmdTimeOut           SFGKey: $Global:SFG.Sql.Timeout
    .PARAMETER Connection
        An existing open connection to use for this query.
        Aliases: Conn, SqlConn        SFGKey: $Global:SFG.Sql.Connection
    .PARAMETER ConnectionString
        Connection string to use for the connection. Credential may be embedded or passed in the Credential parameter- don't do both. If the -IntegratedSecurity switch is set, any embedded credential will be removed and Integrated Security will be added to the connection string.
        Aliases: ConnStr, SqlConnStr  SFGKey: $Global:SFG.Sql.ConnectionString
        NOTE: Integrated Security will NOT be automatically added if using a connection string unless the -IntegratratedSecurity switch is used.
    .PARAMETER Credential
        Credential for connection - overridden by -IntegratedSecurity switch if both present. Do not embed the credential in the connection string if you are passing it separately.
        Aliases: Cred, SqlCred       SFGKey: $Global:SFG.Sql.Credential
    .PARAMETER IntegratedSecurity
        Use IntegratedSecurity -- even if a Credential is present. This is automatically assigned if using a Server/Database and no Credential.
        Aliases: TrustedConnection   SFGKey: $Global:SFG.Sql.IntegratedSecurity
    .PARAMETER Server
        If not using a connection string, this is the server for the connection.
        Aliases: SqlSrv              SFGKey: $Global:SFG.Sql.Server
    .PARAMETER Database
        If not using a connection string, this is the database for the connection.
        Aliases: SqlDB               SFGKey: $Global:SFG.Sql.Database
    .INPUTS
        None.
    .OUTPUTS
        A DataSet (when multiple tables are returned), DataTable (when 1 table is returned), or returned object for non-tabular queries.
    .EXAMPLE
        PS:\>$qry = "SELECT FirstName, LastName, Department FROM EmpTable WHERE Department = @dept"
        PS:\>$data = Invoke-SqlQuery -Query $qry -Parameters @{ 'dept' = "Finance" } -Server Srv1 -DataBase EmpDB

        Run a 'Reader' (default) TEXT query using a parameterized argument and Integrated Security.
    .EXAMPLE
        # Typical scenario for use in SFG module based script. 
        
         # Get the registry credential using the PSAuth system
        $Credential = Get-PSAuthCredential -KeyName 'AccountMgt' -Tag 'ErpDbCreds'

         # Set default values to use for Invoke-SqlQuery calls
        Set-SqlDefaults -Server 'Srv1' -Database 'EmpDb' -Credential $Credential
           # or with a connection string 
        Set-SqlDefaults -ConnectionString 'Data Source=Srv1;InitialCatalog=EmpDB;' -Credential $Credential
           # or with integrated security (note: Integrated is auto-applied if Server/Database and no Credential)
        Set-SqlDefaults -Server 'Srv1' -Database 'EmpDb' -IntegratedSecurity

         # DB and creds are now pre-loaded. During script execution, minimal query parameters are needed:
        $tbl1 = Invoke-SqlQuery `
         "SELECT FirstName, LastName, Department FROM EmpTable WHERE Department = @dept" `
           @{ dept = "Finance" }

        # same creds and server but overriding preset DB setting and relying on -Reader default
        $tbl2 = Invoke-SqlQuery "EXEC sp_ThatLoadsTable2 @yr, @dept" `
          @{ yr = 2024; dept = 'Finance'} -Database 'BudgetDB' 

        # update query
        $rows = Invoke-SqlQuery -NonQuery "UPDATE emps SET ReviewDate = @revDate WHERE Dept = @dept" `
         @{ revDate = $($(Get-Date).AddDays(90).ToString("MM-dd-yyyy"); dept = 'Finance' }

        By setting defaults, fewer args required for Invoke-SqlQuery calls as well as better password security.
    .EXAMPLE
        $topSal = Invoke-SqlQuery -Scalar -Query "SELECT MAX(Salary) FROM EmpTable WHERE Department = 'Sales'" -Connection $conn

        Run a Scalar query to find the top salary being paid to a Sales employee using an existing open connection.
    .NOTES
        Author: Mike Dumdei
        Aliases: RunSql
    #>    
    [CmdletBinding()]
    param (
        [Parameter(ParameterSetName='ReaderConnStr')]
        [Parameter(ParameterSetName='ReaderConn')]
        [Parameter(ParameterSetName='ReaderSrvDB')][Switch]$Reader,
    
        [Parameter(ParameterSetName='ScalarConnStr',Mandatory)]
        [Parameter(ParameterSetName='ScalarConn',Mandatory)]
        [Parameter(ParameterSetName='ScalarSrvDB',Mandatory)][Switch]$Scalar, 
    
        [Parameter(ParameterSetName='NonQueryConnStr',Mandatory)]
        [Parameter(ParameterSetName='NonQueryConn',Mandatory)]
        [Parameter(ParameterSetName='NonQuerySrvDB',Mandatory)][Switch]$NonQuery, 

        [Parameter(Position=1)][Alias('SqlQuery','QueryStr')][string]$Query,
        [Parameter(Position=2)][Alias('SqlParams','Params')][Object]$Parameters, 
    
        [Parameter()][Alias('CmdTimeOut')][int]$TimeOut,

        [Parameter(ParameterSetName='ReaderConnStr', Mandatory)]
        [Parameter(ParameterSetName='ScalarConnStr', Mandatory)]
        [Parameter(ParameterSetName='NonQueryConnStr', Mandatory)]
         [Alias('SqlConnStr', 'ConnStr')][string]$ConnectionString,
    
        [Parameter(ParameterSetName='ReaderConn', Mandatory)]
        [Parameter(ParameterSetName='ScalarConn', Mandatory)]
        [Parameter(ParameterSetName='NonQueryConn', Mandatory)]
         [Alias('SqlConn', 'Conn')][System.Data.SqlClient.SqlConnection]$Connection,
    
        [Parameter(ParameterSetName='ReaderSrvDB', Mandatory)]
        [Parameter(ParameterSetName='ScalarSrvDB', Mandatory)]
        [Parameter(ParameterSetName='NonQuerySrvDB', Mandatory)]
         [Alias('SqlSrv')][string]$Server,
    
        [Parameter(ParameterSetName='ReaderSrvDB', Mandatory)]
        [Parameter(ParameterSetName='ScalarSrvDB', Mandatory)]
        [Parameter(ParameterSetName='NonQuerySrvDB', Mandatory)]
         [Alias('SqlDB')][string]$DataBase, 
    
        [Parameter()][Alias('SqlCred', 'Cred')]$Credential,
        [Parameter()][Alias('TrustedConnection')][switch]$IntegratedSecurity
    )
    if ([String]::IsNullOrWhiteSpace($Query)) {
        return $null
    }
    $closeConn = $true
    try {
        if ($Connection) {
            $closeConn = $false
            if ($Connection.State -ne 'Open') {
                $Connection.Open()
            }
        } else {
            $openArgs = @{}
            if ($ConnectionString) { $OpenArgs.ConnectionString = $ConnectionString }
            if ($Server) { $OpenArgs.Server = $Server }
            if ($Database) { $openArgs.Database = $Database }
            if ($Credential) { $openArgs.Credential = $Credential }
            if ($IntegratedSecurity) { $openArgs.IntegratedSecurity = $true }
            $Connection = Open-SqlConnection @openArgs
            if (!$Connection) {
                throw "Failed to open SQL connection"
            }
        }
    } catch { throw $_ }

    try {  
        [System.Data.SqlClient.SqlCommand]$cmd = $Connection.CreateCommand()
        if ($TimeOut) { $cmd.CommandTimeout = $TimeOut }
        $Query = $Query.Trim()
        if ( $Query.Substring(0, 5) -eq 'EXEC ' -or $Query.Substring(0, 8) -eq 'EXECUTE ') {
            $cmd.CommandType = [System.Data.CommandType]::StoredProcedure
            $Query = $Query.Substring($Query.IndexOf(' ') + 1)
        } else {
            $cmd.CommandType = [System.Data.CommandType]::Text
        }
        $cmd.CommandText = $Query
        if ($Parameters.Count -gt 0) {
            $Parameters.GetEnumerator() | ForEach-Object {
                $val = $_.Value
                $valType = $val.GetType().Name
                if ($valType -eq 'DataTable') {  # table value parameter
                    $param = New-Object System.Data.SqlClient.SqlParameter("@$($_.Key)", `
                     [System.Data.SqlDbType]::Structured)
                    $param.Value = $val
                    $cmd.Parameters.Add($param) | Out-Null
                } elseif ($valType -in @('SwitchParameter','Boolean')) {
                    $val = @(0, 1)[$val -eq $true]
                    $cmd.Parameters.AddWithValue("@$($_.Key)", $val) | Out-Null
                } else {
                    $cmd.Parameters.AddWithValue("@$($_.Key)", $($_.Value)) | Out-Null
                }
            }
        }
        if (!$Reader -and !$Scalar -and !$NonQuery) {
            $Reader = $true
        }
        if ($Reader) {                      # returns DataSet
            [System.Data.DataSet]$dset = New-Object System.Data.DataSet
            [System.Data.SqlClient.SqlDataAdapter]$da = New-Object System.Data.SqlClient.SqlDataAdapter($cmd)
            $da.Fill($dset) | Out-Null
            if ($dset.Tables.Count -eq 1) {
                $rval = $dset.Tables[0]
            } else {
                $rval = $dset
            }
            return , $rval                  
        } elseif ($NonQuery) {              # returns count of rows affected
            $rval = $cmd.ExecuteNonQuery()
        } else {                            # pull single value
            $rval = $cmd.ExecuteScalar()
        } 
        if ($sdbg) { $sdbg.data = $rval }
        return $rval
    } catch { 
        throw $_   # may get to this point if running commands from command prompt
    } finally { 
        if ($null -ne $Connection -and $closeConn -eq $true) { 
            $Connection.Close() | Out-Null; $Connection.Dispose() | Out-Null
        }
    }
}

function Convert-ListToDataTable {
    <#
    .SYNOPSIS
        Converts a PSCustomObject or simple one column array object to a DataTable.
    .DESCRIPTION
        Conversion utility to convert PSCustomObjects, such as imported CSV files, or single column arrays into a DataTable object. The objective is to provide a mechanism to be able to use those objects as sources for table value parameters.
    .PARAMETER InputObject
        The array of objects to be converted. 
        Aliased as 'Ary', 'Array', 'Data'.
    .PARAMETER MapTable
        A table containing the column names and data types to be used for the resulting DataTable. If not specified, the column names and types are derived from the InputObject. MapTables need a 'name' and 'type' column. The 'name' column contains the column name and the 'type' column contains the data type. It may also have 'map' column that maps the column name in the InputObject to a column field in the DataTable. Data column names match the PSObject property name if not mapped. The MapTable may be created from an existing SQL table or table value parameter type using the Get-TableSchema function.
    .PARAMETER OmitColumns
        If a column is in this list, do not include it in the DataTable. This is useful if you have a PSCustomObject with columns you don't want to include in the DataTable.
    .PARAMETER IncludeColumns
        If a column is in this list, include it in the DataTable. This is useful if you have a PSCustomObject with columns you don't want to include in the DataTable.
    .PARAMETER OmitObjectTypes
        If a column type is not in the list below, do not include it in the DataTable. 
         
        [String], [Decimal], [Int32], [Int64], [Double], [Single], [DateTime], [TimeSpan], [Boolean], [Guid]

        Without this parameter, all columns are included and those not listed above are converted to JSON and stored as a string.
    .PARAMETER ColumnName
        If the item being converted is a PSCustom object, the column names are derived from the object. For single column arrays, however, this parameter allows you to assign a field name to the result.
        Aliased as 'ColName', 'Name'.
    .EXAMPLE
        PS:\>$data = Import-Csv file.csv
        PS:\>$tbl = Convert-ListToDataTable $data
        PS:\>Invoke-SqlQuery -Reader -Query "EXEC sp_xyz" -Parameters @{ spParam1 = $tbl } -SqlConnection $conn

        Convert a CSV to a table value parameter and pass it as a parameter to a stored procedure. Requires table type to be created and a stored procedure that takes that TVP type as a parameter.
        SQL side requirements for this example:
        CREATE TYPE myTvpType AS TABLE(last varchar(50), first varchar(30))
        GO
        CREATE PROCEDURE sp_xyz @spParam1 myTvpType READONLY AS 
        BEGIN
            SELECT id_num, last_name, first_name 
            FROM namemaster n JOIN @spParam1 p ON n.last_name = p.last AND n.first_name = p.first
        END
        GO
        GRANT EXEC ON sp_xyz TO [** you **]
        GRANT EXEC ON TYPE::myTvpType TO [** you **]
    .NOTES
        Author: Mike Dumdei
    #>
    param(
        [Parameter(Mandatory,Position=0)][Alias('Ary','Array','Data')][Object[]]$InputObject, 
        [Parameter()][string[]]$OmitColumns,
        [Parameter()][string[]]$IncludeColumns,
        [Parameter()][switch]$OmitObjectTypes,
        [Parameter()][Object[]]$MapTable,
        [Parameter()][string][Alias('ColName','Name')][string]$ColumnName = 'col1'
    )
    begin {
        $tmap = @{
            String = 'String'; Decimal = 'Decimal'; Int32 = 'Int32'; Int64 = 'Int64';
            Double = 'Double'; Single = 'Single'; DateTime = 'DateTime'; 
            TimeSpan = 'TimeSpan'; Boolean = 'Boolean'; Guid = 'Guid';
            char = 'String'; varchar = 'String'; nchar = 'String'; nvarchar = 'String';
            numeric = 'Decimal'; # decimal = 'Decimal'
            int = 'Int32'; bigint = 'Int64'; smallint = 'Int32'; tinyint = 'Int32';
            float = 'Double'; real = 'Single';
            datetime2 = 'DateTime'; datetimeoffset = 'DateTime'; time = 'TimeSpan';
            bit = 'Boolean'; uniqueidentifier = 'Guid'; # datetime = 'DateTime';
        }
    }
    process {
        $dataType = $InputObject[0].GetType().Name
        if ($dataType -eq 'DataTable') {                # already a DataTable
            return @(,$InputObject)
        }
        if ($dataType -in @('String','Decimal','Int32','Int64','Double','Single','DateTime','TimeSpan','Boolean','Guid')) {   
            $properties = $null                         # is an array of simple types
        } else {
            try {                                       # is an array of objects
                $properties = ([pscustomobject]($InputObject[0])).PSObject.Properties
                if ($properties.Count -eq 0) { $properties = $null }
            } catch { $properties = $null }
        }
        if ($properties) { 
            $cNm = New-Object 'System.Collections.Generic.Dictionary[string,string]'
            $cType = New-Object 'System.Collections.Generic.Dictionary[string,string]'
            if ($MapTable -and $MapTable.name) {
                $MapTable | ForEach-Object {
                    # key = psobject name, value = datatable name, datatable type
                    if ($null -eq $_.map) { $_.map = $_.name }
                    $typ = @($tmap[$_.type],'String')[$null -eq $tmap[$_.type]]
                    $cNm.Add($_.map, $_.name)
                    $cType.Add($_.map, $typ)
                }
            } else {
                $properties | ForEach-Object {
                    # key = datatable = psobject property name, type from psobject property type
                    $prop = $_.Name; 
                    $v = $($InputObject | Where-Object { $null -ne $_.$prop } | Select-Object -First 1).$prop
                    if ($v) { $v = $v.GetType().Name; $typ = @($tmap[$v],'String')[$null -eq $tmap[$v]] }
                     else { $typ = 'String' }
                    $cNm.Add($_.Name, $_.Name)
                    $cType.Add($_.Name, $typ)
                }
            }
            [System.Data.DataTable]$tbl = New-Object System.Data.DataTable
            $cType.GetEnumerator() | ForEach-Object {
                if (($null -eq $IncludeColumns -or $IncludeColumns -contains $_.Key) `
                -and ($null -eq $OmitColumns -or $OmitColumns -notcontains $_.Key) `
                -and (!$OmitObjectTypes -or $cType[$_.Value] -ne [Object])) {
                    $tbl.Columns.Add($(New-Object System.Data.DataColumn($_.Key, $_.Value)))
                }
            }
            $colNames = $tbl.Columns.ColumnName
            $MapTable `
             | Where-Object { $_.length -and $_.length -ne -1 -and $_.map -in $colNames } `
             | ForEach-Object { $tbl.Columns[$_.map].MaxLength = $_.length }
            foreach ($itm in $InputObject) {
                [System.Data.DataRow]$r = $tbl.NewRow()
                $colNames | ForEach-Object { 
                    if ($null -eq $itm.($_)) { 
                        $r[$_] = [DBNull]::Value 
                    } elseif ($tbl.Columns[$_].DataType.Name -eq 'String') {
                        if ($_ -is [string]) { $r[$_] = $itm.($cNm.$_) }
                         else { $r[$_] = ConvertTo-Json $itm.($_) -Depth 3 -Compress
                        }
                    } else {
                        $r[$_] = $itm.($cNm.$_)
                    }
                }
                $tbl.Rows.Add($r)
            }
        } else {  # this assumes single column array with same data type for all elements
            [System.Data.DataTable]$tbl = New-Object System.Data.DataTable
            $tbl.Columns.Add($(New-Object System.Data.DataColumn($ColumnName, $InputObject[0].GetType().Name)))
            $InputObject | ForEach-Object {
                [System.Data.DataRow]$r2 = $tbl.NewRow()
                $r2[$ColumnName] = [Convert]::ChangeType($_, $_.GetType())
                $tbl.Rows.Add($r2)
            }
        }
        # without the comma, the DataTable gets converted to an array of Objects
        # when returned - defeats the purpose
        return @(,$tbl)
    }
}

function Get-TvpTypeDDL {
<#
.SYNOPSIS
    Generates the DDL to make a table value parameter type.
.DESCRIPTION
    Given a name for the TVP type and a DataTable, this will create the DDL to create a TVP type for the table. Typical use is convert a PSCustomObject or imported CSV to a DataTable (Convert-ListToDataTable) and use that DataTable as the input to this function.
.PARAMETER TvpName
    The name for the table value type. It may be proceeded with a schema if not using 'dbo'. These are created in SSMS under 'DBName\Programmability\Types\User-Defined Table Types'.
.PARAMETER Table
    The datatable with the data elements you are wanting to use as the source structure for the DDL.
.PARAMETER WithNoReplace
    Prevents overwriting an existing TVP if one exists with the same name in the same schema.
.PARAMETER WithDrop
    Drops the TVP if it exists and recreates it with the new definition.
.PARAMETER szVarchar
    The size argument for the VARCHAR when converting String values - defaults to 'max', 512 may be more appropriate.
.PARAMETER szDecimal
    The size argument for DECIMAL when converting Decimal values - defaults to '18,2' - the default is probably way to big.
.EXAMPLE
    PS:\> Get-TvpTypeDDL -TvpName 'dbo.myTvpType' -Table $someTable -WithDrop -szVarchar 512 -szDecimal '10,2'

    Generates the DDL statements to create a Table type for use with table valued parameters.
.NOTES
    Author: Mike Dumdei    
#>    
    param([string]$TvpName, [System.Data.DataTable]$Table, [switch]$WithNoReplace, [switch]$WithDrop, $szVarchar = 'max', $szDecimal = '18,2')
    $map = @{ 'String' = "VARCHAR($szVarchar)"; 'Decimal' = "DECIMAL($szDecimal)"
              'Int32' = "INT"; 'Int64' = "BIGINT"; 'Double' = "FLOAT"; 'Single' = "FLOAT"; 
              'DateTime' = "DATETIME"; 'TimeSpan' = "TIME"; 'Boolean' = "BIT"; 'Guid' = "UNIQUEIDENTIFIER" }
    [string[]]$sAry = $TvpName.Replace("[","").Replace("]","").Split('.')
    if ($sAry.Length -lt 2) {
        $TvpSchema = 'dbo'
    } else {
        $TvpSchema = $sAry[0]
        $TvpName = $sAry[1]
    }
    $ddl = ""
    if ($WithNoReplace) {
        $ddl += `
         "IF NOT EXISTS (SELECT 1 FROM sys.table_types t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE t.name = '$TvpName' AND s.name = '$TvpSchema')`r`n"
    }
    if ($WithDrop) {
        $ddl += "DROP TYPE IF EXISTS [$TvpSchema].[$TvpName]`r`n"
    }
    $ddl += "CREATE TYPE [$TvpSchema].[$TvpName] AS TABLE ("
    foreach ($c in $Table.Columns) {
        $ddl += "`r`n  [$($c.ColumnName)] $($map[($c.DataType).Name]),"
    }
    $ddl = $ddl.TrimEnd(',') + "`r`n)"
    return $ddl
}

function GenerateTemplateSQL {
    $ddl = "`r`n-- Create TableType based on CSV file: $CsvFile`r`n"
    $ddl += "-- ** Adjust data types and sizes to something logical **`r`n"
    $ddl += "-- DROPs are to clean up and start from scratch`r`n`r`n"
    $ddl += "DROP PROCEDURE IF EXISTS $ProcedureName`r`n"
    $ddl += Get-DDL 
    if ($null -eq $ddl -or $ddl -notlike "*CREATE TYPE*") {
        "Failed to create TableType DDL - check for valid CSV file" | Out-Host
        return
    }
    if ($CreateUploadDDL) {    # create Load table from CSV template
        $ddl += @"

GO

/****************************************************************************
*  $ProcedureName - upload SP for $TableTypeName type CSV files
*****************************************************************************/
CREATE PROCEDURE [$ProcedureName] 
 @csv AS [$TableTypeName] READONLY, -- Input CSV data passed in the query
 @create AS VARCHAR(1),             -- Delete and recreate table (admin)
 @truncate AS VARCHAR(1)            -- Truncate without delete
AS BEGIN
   -- zap table completely if 'create' option was set
  IF @create = 1 DROP TABLE IF EXISTS [$TableName] 
   -- if the target table doesn't exist, create empty table based on type definition 
  IF OBJECT_ID('$TableName', 'U') IS NULL BEGIN
    DECLARE @t AS [$TableTypeName]
    SELECT * INTO [$TableName] FROM @t WHERE 1 = 0
  END
   -- zap table contents if truncate option was set
  IF @create <> 1 AND @truncate = 1 DELETE FROM [$TableName] WHERE 1 = 1
   -- load the CSV data into the specified table
  INSERT INTO [$TableName]      -- a column list could be provided here
    SELECT * FROM @csv          -- and specific columns to use from the csv here
END
GO

"@
    } elseif ($CreateProcessDDL) {
        $ddl += @"

GO

/****************************************************************************
*  $ProcedureName - SP for processing $TableTypeName type CSV files
*****************************************************************************/      
CREATE PROCEDURE [$ProcedureName] 
 @csv AS [$TableTypeName] READONLY  -- Input CSV data passed in the query
AS BEGIN
   -- The idea here is you JOIN and use @csv just like any other table
   -- SELECT, UPDATE, INSERT, DELETE, acres of SQL statements - whatever
  SELECT *
  FROM @csv c
   JOIN j1Table j ON c.column = j.colunn
  WHERE c.column2 = 'Y'
END
GO

"@
   }
   $ddl += @"

-- Optional - Create the user account to run the procedure
-- USE SET_DBNAME_HERE
-- GO
-- CREATE LOGIN $AuthorizedUser WITH PASSWORD = 'SET_PWD_HERE';
-- CREATE USER $AuthorizedUser FOR LOGIN $AuthorizedUser;

-- Grant EXEC to the table type and stored procedure to the authorized user/role   
GRANT EXEC ON TYPE::[$TableTypeName] TO [$AuthorizedUser]
GRANT EXEC ON [$ProcedureName] TO [$AuthorizedUser]`r`n
"@   
    $ddl
}

#############################################################################
#  Script main process starts here
#############################################################################
iF (!$(Test-Path $CsvFile -PathType Leaf)) {
    "The CSV file does not exist: [" + $CsvFile + "]" | Out-Host
    return
}

if ($PSCmdlet.ParameterSetName -like "*DDL*" ) {
    GenerateTemplateSQL
} else {
    $sqlArgs = @{ 'Server' = $Server; 'DataBase' = $Database }
    if ($Credential) { $sqlArgs.Credential = $Credential }
    $csv = Import-Csv $CsvFile
    $tbl = Convert-ListToDataTable $csv
    $spArgs = @{ 'csv' = $tbl;  }
    if ($Upload) {
        $spArgs.create = $($Create -eq $true)
        $spArgs.truncate = $($Truncate -eq $true)
    }
    if ($ReturnRows) {
        Invoke-SqlQuery -Reader -Query "EXEC $ProcedureName" -Parameters $spArgs @sqlArgs
    } else {
        Invoke-SqlQuery -NonQuery -Query "EXEC $ProcedureName" -Parameters $spArgs @sqlArgs
    }
}

