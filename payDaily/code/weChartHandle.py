import itchat
import time
import wxpy


#import wechatpy
#from wechatpy.replies import TextReply
'''
reply = TextReply()
reply.source = 'user1'
reply.target = '我爱罗'
reply.content = 'hello'

xml = reply.render()


'''
bot = wxpy.Bot(console_qr = 2, cache_path = 'botto.pk')

def main():
	itchat.auto_login(enableCmdQR=True)
	itchat.send("hello","我爱罗")
	print("Message is send successed")

friend = bot.friends().search('我爱罗')
friend.search("hello")



if __name__ == '__main__':
	#main()
	pass
