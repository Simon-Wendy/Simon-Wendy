import smtplib
#发送字符串邮件
from email.mime.text import MIMEText
#处理多种邮件主题我们需要的MIMEMultipart
from email.mime.multipart import MIMEMultipart
#处理图片类
from email.mime.image import MIMEImage
#处理带附件邮件
from email.mime.application import MIMEApplication
#授权码 nvlcaywcpsfqfhig
#	ujnulzfqgbfnjgdf
from email.header import Header

'''
#设置服务器所需信息
sender = "1392535794@qq.com"  #邮件发送者
passWord = ""   
qqCode = "pmpcciytlxmihgbf"#授权码
smtp_port = 465 #固定端口
smtp_server = "smtp.qq.com"
#设置Email信息

#邮件内容
message = MIMEText("Hello,人生苦短 我用Python","plain","utf8")
#邮件发送人
message["from"] = ("%s<系统邮件>") % Header("腾讯官方","utf8")
#邮件主题
message["Subject"] = "My First Email"
#邮件接收方
#message["To"] = Header("张燕青","utf-8")
message["To"] = "张燕青"


#登录并发送邮件
try:
	server = smtplib.SMTP_SSL(smtp_server,smtp_port) #邮箱服务器地址，端口默认为25
	server.login(sender,qqCode)
	server.sendmail(sender,["zhangyanqing@ruizhiqi.com",],message.as_string())
	print("Success")
	server.quit()
except smtplib.SMTPException as e:
	print("error",e)
'''


class AutSendEmail(object):
	"""docstring for autSendEmail"""
	def __init__(self, data,toSender):
		super(AutSendEmail, self).__init__()
		self.mail = "GoodMoring Sir：" + "\n" + "  "*4 + data
		self.toSender = toSender
		self.sender = "1392535794@qq.com"
		self.qqCode = "pmpcciytlxmihgbf"
		self.message = MIMEText(self.mail,"plain","utf-8")
		self.message["from"] = ("%s<自动发送>") % Header("系统邮件","utf8")
		self.message["Subject"] = "The more the better"
		#message["To"] = ""
	def sendMessage(self):
		try:
			server = smtplib.SMTP_SSL("smtp.qq.com",465)
			server.login(self.sender,self.qqCode)
			server.sendmail(self.sender,self.toSender,self.message.as_string())
			print("Send email Success")
			server.quit()
		except smtplib.SMTPException as e:
			print("发送失败，请检查原因：",e)
 




if __name__ == '__main__':
	sendMessage = AutSendEmail("guess me",["zhangyanqing@ruizhiqi.com"])
	sendMessage.sendMessage()
