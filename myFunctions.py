import re


def extract_languages(string):
    lang_regex = r"Java|Python|C\+\+|C\#|Go|Ruby|Javascript|PHP|HTML|CSS|SQL"
    if string is not None:
        return re.findall(lang_regex, string)


def extract_domains(string):
    domains_regex = r"href=\"(\S+)\""
    domains = re.findall(domains_regex, string)
    output = []
    # Tìm các string có dạng href="..."
    # ... có dạng http:// hoặc https:// nên split và lấy index 2 để được tên miền.
    # Tuy nhiên phải đặt trong khối try... except để tránh các string gây lỗi index.
    for domain in domains:
        try:
            output.append(domain.split('/')[2])
        except IndexError as e:
            pass
            # print(e, domain)
    return output


# test = "<p>I'm setting up a dedicated SQL Server 2005 box on Windows Server 2008 this week, and would like to pare
# it " \ "down to be as barebones as possible while still being fully functional.</p>\n\n<p>To that end, the \"Server
# " \ "Core\" option sounds appealing, but I'm not clear about whether or not I can run SQL Server on that SKU.  " \
# "Several services are addressed on the <a " \
# "href=\"http://www.microsoft.com/windowsserver2008/en/us/compare-core-installation.aspx\">Microsoft " \
# "website</a>, but I don't see any indication about SQL Server.</p>\n\n<p>Does anyone know definitively?</p>\n "

'''
test_domains = """80,26,2008-08-01T13:57:07Z,NA,26,SQLStatement.execute() - multiple queries in one statement,
"<p>I've written a database generation script in <a href=""http://en.wikipedia.org/wiki/SQL"">SQL</a> and want to 
execute it in my <a href=""http://en.wikipedia.org/wiki/Adobe_Integrated_Runtime"">Adobe AIR</a> application:</p> 

<pre><code>Create Table tRole (
      roleID integer Primary Key
      ,roleName varchar(40)
);
Create Table tFile (
    fileID integer Primary Key
    ,fileName varchar(50)
    ,fileDescription varchar(500)
    ,thumbnailID integer
    ,fileFormatID integer
    ,categoryID integer
    ,isFavorite boolean
    ,dateAdded date
    ,globalAccessCount integer
    ,lastAccessTime date
    ,downloadComplete boolean
    ,isNew boolean
    ,isSpotlight boolean
    ,duration varchar(30)
);
Create Table tCategory (
    categoryID integer Primary Key
    ,categoryName varchar(50)
    ,parent_categoryID integer
);
...
</code></pre>

<p>I execute this in Adobe AIR using the following methods:</p>

<pre><code>public static function RunSqlFromFile(fileName:String):void {
    var file:File = File.applicationDirectory.resolvePath(fileName);
    var stream:FileStream = new FileStream();
    stream.open(file, FileMode.READ)
    var strSql:String = stream.readUTFBytes(stream.bytesAvailable);
    NonQuery(strSql);
}

public static function NonQuery(strSQL:String):void
{
    var sqlConnection:SQLConnection = new SQLConnection();
    sqlConnection.open(File.applicationStorageDirectory.resolvePath(DBPATH);
    var sqlStatement:SQLStatement = new SQLStatement();
    sqlStatement.text = strSQL;
    sqlStatement.sqlConnection = sqlConnection;
    try
    {
        sqlStatement.execute();
    }
    catch (error:SQLError)
    {
        Alert.show(error.toString());
    }
}
</code></pre>

<p>No errors are generated, however only <code>tRole</code> exists. It seems that it only looks at the first query (
up to the semicolon- if I remove it, the query fails). Is there a way to call multiple queries in one statement?</p> """
'''
