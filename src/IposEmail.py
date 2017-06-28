# -*- coding:UTF-8 -*-
'''
说明:检查Ipos上传给商城数据任务是否关闭
每天早上八点检查
 
'''

import datetime,time
from email.header import Header
from email.mime.text import MIMEText
from java.lang import Class
from java.net import ConnectException
from java.sql import DriverManager
from java.util import ArrayList, HashMap
import smtplib
import sys
from email.mime.application import MIMEApplication

defaultencoding = 'utf8'
if sys.getdefaultencoding() != 'utf8':
    reload(sys)
    sys.setdefaultencoding = defaultencoding

# 取得数据库的连接
def getConnect():
#     resultConnection = odiRef.getJDBCConnection("MOMPROD")
#     return resultConnection
    url = 'jdbc:mysql://172.16.0.168:3306/ipos?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true'
    user = 'ipos'
    password = 'baison8888'
    try:
        Class.forName('com.mysql.jdbc.Driver')
        Connection = DriverManager.getConnection(url, user, password)
    except ConnectException:
        print "Error:Connect Failed"    
    return Connection

# 执行 SQL 查询语句    
def exeSql(sql):  
    resultList = ArrayList()
    targetConnection = getConnect()
    resultStatement = targetConnection.createStatement()
    resultSet = resultStatement.executeQuery(sql)
    fileds = resultSet.getMetaData()
    
    while resultSet.next():
        bean = HashMap()
        size = fileds.getColumnCount() # 取得列数
        
        for i in range(0, size):
            colName = fileds.getColumnLabel(i+1)
            colData = resultSet.getObject(i+1)
            bean.put(colName, colData)
        resultList.add(bean)
    
    resultSet.close()
    resultStatement.close()
    return resultList   

# 执行 SQL 查询语句    
def update(sql):
    targetConnection = getConnect()
    resultStatement = targetConnection.createStatement()
    resultStatement.executeUpdate(sql)
    

  
#发送失败邮件  
def sendEmail(content):
    # 开启 SMTP 
    mail_host = 'mail.winnermedical.com' 
    mail_user = 'jyye'
    mail_pass = '44rrFF'
    
    # 设置发送人
    sender = 'jyye@winnermedical.com'
    # 设置接受人
    to_addr1 = '739802212@qq.com'
    
    
    # 内容
    message = MIMEText(content, 'plain', 'UTF-8')
    # 消息编码
    message['Accept-Language'] = 'zh-CN'
    message['Accept-Charset'] = 'ISO-8859-1,UTF-8'
    
    # 设置发件人
    message['From'] = '<'+sender+'>'
    # 设置收件人
    message['To'] = ','.join([to_addr1])
    
    # 设置邮件标题
    subject = 'IPOS项目数据传送失败'
    message['Subject'] = Header(subject, 'UTF-8')
    # #定义附件内容 xls类型附件
    # 
    # part = MIMEApplication(open('filename','rb').read())
    # part.add_header('Content-Disposition', 'attachment', filename='filename')
    # message.attach(part)
    
    # 开启邮箱服务
    server = smtplib.SMTP(mail_host, 25)
    # 登录邮箱
    server.login(mail_user, mail_pass)
    # 发送邮件
    server.sendmail(sender, [to_addr1], message.as_string() )
    # 关闭邮箱服务
    server.close()
    
    
content =''  

#查询未传送过去的数据条数
sql_new = '''select *
          from ipos_qtlsd m
         where zddm = '100021'
           and gd = 0
           and rq >= UNIX_TIMESTAMP('2017-06-26') 
           and id not in ( select id from tbl_transfer_temp)
             AND round((select sum(je)from ipos_qtlsdjs 
             where pid = m.id and js_id in (1,2,5,1003,1004,1005,1012,1014,1022)),2) is not null''' 
             
sql_update="UPDATE quartz_schedulejob  SET cron_expression='0 00,30 09 * * ?' WHERE job_name='shenZhenKKMall' "   

datas_new = exeSql(sql_new)
size_new = datas_new.size()

if size_new > 20:
    content="门店数据上传任务可能关闭,请及时查看"
    update(sql_update)
    sendEmail(content)
    
    
    
    



   

