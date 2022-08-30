import pandas as pd
import numpy as np
import time
from autoSendEmail import AutSendEmail
import datetime

def emailData():#编辑邮件发送内容
    total,ratio,top_up,top_down,yafang,newQ,xinkai,dataF,dataFive = dataHandle()
    print('='*20,'昨日消费情况','='*20)
    print(dataFive)
    print(dataF)
    print("*"*50)
    print('total',total)
    print("-"*50)
    print('ratio',ratio)
    print("=" * 50)
    print(top_up.sort_values(by="消费环比",ascending=False).head(10))
    print("=" * 50)
    print(top_down.sort_values(by="消费环比").head(10))
    print("季度新户：\n",newQ)
    print("新开客户消费：\n",xinkai)
    data_1 ="\n" + "昨日消费情况:" + "\n" + total.to_string() + "\n" + "昨日消费环比" + "\n" + ratio.to_string() + "\n" + "环比涨客户:" + "\n" + top_up.sort_values(by="消费环比",ascending=False).head(10).to_string() + "\n" + "环比下降客户:" + "\n" + top_down.sort_values(by="消费环比").head(10).to_string()
    data_2 = "\n" + "昨日雅方消费情况：" + "\n" + str(yafang) + '\n' + "季度新户情况：" + '\n' + newQ.to_string() + '\n' + "新开客户消费：" + '\n' + xinkai.to_string()
    dataall = dataFive + '\n' + dataF
    return  dataall, dataall + data_1 + data_2



def dataHandle():#处理数据，使其变成理想数据
    data_finally = pd.DataFrame()
    data_finally_a = pd.DataFrame()
    column_1 = ["郑州雅方教育研究院(普通合伙)", "郑州雅方教育信息咨询有限公司","郑州雅方心理咨询有限公司","郑州市雅方教育科技有限公司"]
    # column_4 = ["北京市盈科(郑州)律师事务所"]
    # 中小转户客户
    '''
    column_5 = ["河南轩仁律师事务所","河南开合电子数据司法鉴定所","张胜利",'河南振豫律师事务所','河南植尚律师事务所','河南普丰律师事务所','河南焕廷律师事务所',
                '河南中冶(郑州)律师事务所','河南锦盾律师事务所','河南智举律师事务所','河南曲成律师事务所','河南新动力律师事务所','河南修谨律师事务所','王大利',
                '北京市京师(郑州)律师事务所','张冬冬','河南九博律师事务所','河南亚太人律师事务所','河南家港律师事务所','河南沃华律师事务所','河南通利元法律服务有限公司',
                '王亚洲','王静','白威威','袁延顶','谢建宏','史玉靖','吴天阔','崔世杰','张海鹏','李明月','李月','河南扬善律师事务所','河南程功律师事务所','王艳慧','白爱敏']
    '''
    # 10月转户
    # column_2 = ["开封中影人教育咨询有限公司", "河南引导拓展训练基地有限公司","河南笨笨熊商贸有限公司","许昌驰诚电气有限公司",'驻马店申正法医临床司法鉴定所','平顶山路衢网络科技有限公司']
    # 11月转户
    # column_2 = ["永城市福万康电子科技有限公司",'河南沐之鑫实业有限公司']

    dtime = time.strftime("%Y%m%d", time.localtime())
    qtime = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=182),"%Y%m%d")
    qdata = pd.DataFrame([dtime,qtime],columns=['dataflag'])
    qdata['dataflag'] = pd.to_datetime(qdata.dataflag,format='%Y%m%d')
    qdata['qur'] = pd.PeriodIndex(qdata.dataflag,freq='q')
    q1 = qdata.qur[0]
    q2 = qdata.qur[1]
    print('='*20,q1,q2)
    path = str(dtime) + ".csv"
    now_day = int(datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(days=1),"%Y%m%d"))
    yet_day = int(datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(days=2),"%Y%m%d"))
    df = pd.read_csv("../data/{}".format(path))
    df["总消"] = df["总消费"] - df["凤巢优惠券消费"] - df["原生CPC优惠券消费"] - df["原生CPM优惠券消费"]
    df["信息流"] = df["阿拉丁CPT消费"] + df["原生阿拉丁消费"] + df["原生CPC消费"] + df["原生CPM消费"] + df["百意Feed消费"] + df[
        "百意无线开屏CPC消费"] + df["原生GD消费"] + df["百意FeedGD消费"] - df["原生CPC优惠券消费"] - df["原生CPM优惠券消费"] + df["手百开屏消费"]
    df["搜索消费"] = df["搜索点击消费"] + df["凤巢阿拉丁消费"] - df["凤巢优惠券消费"]
    data = df[["data_flag","账户名称","客户名称", "总消", "信息流", "搜索消费"]].copy()

    #雅方消费
    yafang = []
    for col in column_1:
        yafang_total = data[(data["客户名称"] == col) & (data["data_flag"] == now_day)][["总消","信息流","搜索消费"]].sum()
        yafang.append([col, yafang_total])
    #print("雅方消费情况：",yafang)

    #盈科消费减半
    # for col in column_4:
    #     data.loc[data["客户名称"] == col, "总消"] = (data[data["客户名称"] == col]["总消"]) / 2
    #     data.loc[data["客户名称"] == col, "信息流"] = (data[data["客户名称"] == col]["信息流"]) / 2
    #     data.loc[data["客户名称"] == col, "搜索消费"] = (data[data["客户名称"] == col]["搜索消费"]) / 2


    #2021Q3律师转户不算消费
    '''
    for col in column_5:
        data.loc[data["客户名称"] == col, "总消"] = 0
        data.loc[data["客户名称"] == col, "信息流"] = 0
        data.loc[data["客户名称"] == col, "搜索消费"] = 0
    '''
    # 11月转户不算业绩
    # transfer_11 = []
    # for col in column_2:
    #     total_11 = data[(data["客户名称"] == col) & (data["data_flag"] == now_day)][["总消","信息流","搜索消费"]].sum()
    #     transfer_11.append([col, total_11])
    # print("11月转户消费：\n",transfer_11)
    # for col in column_2:
    #     data.loc[data["客户名称"] == col, "总消"] = 0
    #     data.loc[data["客户名称"] == col, "信息流"] = 0
    #     data.loc[data["客户名称"] == col, "搜索消费"] = 0


    #当天消费情况
    total = data[data["data_flag"] == now_day][["总消", "信息流", "搜索消费"]].sum()
    #消费环比情况
    ratio = data[data["data_flag"] == now_day][["总消", "信息流", "搜索消费"]].sum() - data[data["data_flag"] == yet_day][
        ["总消", "信息流", "搜索消费"]].sum()

    now_data = data[data["data_flag"]==now_day].groupby(by="客户名称").sum()
    yet_data = data[data["data_flag"] == yet_day].groupby(by="客户名称").sum()
    new_data = pd.merge(now_data,yet_data,left_on=now_data.index,right_on=yet_data.index,how="outer")
    data_finally["客户名称"] = new_data["key_0"]
    data_finally["昨日总消"] = new_data["总消_x"]
    data_finally["前日总消"] = new_data["总消_y"]
    data_finally = data_finally.fillna(0)
    data_finally["消费环比"] = data_finally["昨日总消"] - data_finally["前日总消"]



    #Q3季度新户
    newcolum = pd.read_excel(r'../data/季度新户.xlsx')
    newcolum['季度'] = pd.PeriodIndex(newcolum.季度,freq='q')
    newcolum = newcolum[newcolum['季度']>=q2]
    newQ = pd.merge(newcolum,data_finally,how='left',left_on='公司名称',right_on='客户名称').sort_values(by='消费环比',ascending=False)
    corr = pd.read_excel(r'../data/8月对应关系.xlsx')
    corr_c = pd.read_excel(r'../data/对应关系.xlsx')
    corr_a = corr_c[['公司名称', '简称']]
    corr_b = corr[['公司名称','部门','对应销售']]

    data_ = pd.merge(data_finally,corr_b,how='left',left_on='客户名称',right_on='公司名称')
    data_a = pd.merge(data_finally,corr_a,how='left',left_on='客户名称',right_on='公司名称')
    data_a = data_a.groupby('简称').sum()
    data_a = data_a.reset_index()
    xinkai = data_[data_['部门'].isnull()]


    top_cil_down = data_finally[data_finally["消费环比"]<0]  #消费环比小于0的客户
    top_cil_up = data_finally[data_finally["消费环比"] > 0]  #消费环比大于0的客户

    # 增加优化部门消费情况
    optimization = pd.merge(data,corr,how='left',on='账户名称')
    optim = optimization[['data_flag','优化部门','总消']].groupby(by=['data_flag','优化部门']).sum().reset_index()

    nowCon = optim[optim['data_flag']==now_day]
    yetCon = optim[optim['data_flag'] == yet_day]
    optimDepart = ['KA优化一部','KA优化二部']
    optDepStr = '昨日'
    for dep in optimDepart:
        depTotal = nowCon[nowCon['优化部门'] == dep]['总消'].values[0]
        depCon = nowCon[nowCon['优化部门'] == dep]['总消'].values - yetCon[yetCon['优化部门'] == dep]['总消'].values
        ratioStr = ratioIncrease(depCon[0])
        optDepStr  += '{}消费{}万，{}'.format(dep,np.round(depTotal/10000,1),ratioStr)


    #消费大于5000客户
    all_five = data_a[(data_a["消费环比"] >= 2000) | (data_a["消费环比"] <= -2000)].sort_values(by='消费环比',ascending=False)

    top_up = top_cil_up[top_cil_up['消费环比']>2000].sort_values(by='消费环比',ascending=False)
    top_down = top_cil_down[top_cil_down['消费环比']<-2000].sort_values(by='消费环比',ascending=False)
    newD = newQ[abs(newQ['消费环比'])>300]

    dataF = textData(total,ratio,top_up,top_down,newD)

    dataFive = all_five_data(total, ratio, top_up, top_down, all_five,optDepStr)


    return total,ratio,top_cil_up,top_cil_down,yafang,newQ,xinkai,dataF,dataFive
def ratioIncrease(data):
    if data>0:
        textStr = "环比增长{}万；".format(np.round(data/10000, 1))
    else:
        textStr = "环比下降{}万；".format(np.round(abs(data/10000),1))
    return textStr

def textData(total,ratio,top_cil_ip,top_down,newQ):

    dataN = '主要新户消费情况：\n'
    for i in range(newQ.shape[0]):
        r = ''
        if newQ.iloc[i,6]>0:
            r = '增长'
        else:
            r = '下降'
        dataN += '{}昨日消费{}，环比{}{}；'.format(newQ.iloc[i,2],int(newQ.iloc[i,4]),r,
                                          int(abs(newQ.iloc[i,6])))

    dataFinall = dataN
    return dataFinall

def all_five_data(total,ratio,top_cil_ip,top_down,all_five,opt):
    data_str = "昨日总消费{}万，{}".format(np.round(total['总消']/10000,1),ratioIncrease(ratio['总消']))\
               +"信息流{}万，{}".format(np.round(total['信息流']/10000,1),ratioIncrease(ratio['信息流']))\
               +"搜索{}万，{}".format(np.round(total['搜索消费']/10000,1),ratioIncrease(ratio['搜索消费']))

    dataS = '昨日主要增长下降客户：\n'
    for i in range(top_cil_ip.shape[0]):
        dataS += '{}{}'.format(top_cil_ip.iloc[i,0],ratioIncrease(top_cil_ip.iloc[i,3]))

    for i in range(top_down.shape[0]):
        dataS += '{}{}'.format(top_down.iloc[i,0],ratioIncrease(top_down.iloc[i,3]))

    dataN = '昨日主要客户消费情况：\n'
    for i in range(all_five.shape[0]):
        r = ''
        if all_five.iloc[i,3]>0:
            r = '增长'
        else:
            r = '下降'
        dataN += '{}环比{}{}万；'.format(all_five.iloc[i,0],r,np.round(abs(all_five.iloc[i,3])/10000,1))

    dataFinall = data_str + '\n'+ opt +'\n' + dataN
    return dataFinall

def main():

    data1,data2 = emailData()
    sendMessage2 = AutSendEmail(data2,["zhangyanqing@ruizhiqi.com"])
    sendMessage2.sendMessage()



if __name__ == "__main__":
    main()
    # data = [datetime.datetime.now()]
    # df = pd.DataFrame(data,columns=['date'])
    # df['date'] = pd.to_datetime(df.date,format='%Y%m%d')
    # df['qur'] = pd.PeriodIndex(df.date,freq='q')
    # a = df.qur[0]
    # print(df.qur,'\n',a)


