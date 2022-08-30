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
    column_1 = ["郑州雅方教育研究院(普通合伙)", "郑州雅方教育信息咨询有限公司"]
    #column_2 = ["河南华龙电力电缆有限公司","郑州宏亮电缆有限公司","河南华东电缆股份有限公司"]
    #column_3 = ["睢县足力健鞋业有限公司","河南宣和钧釉环保材料有限公司"]
    column_4 = ["北京市盈科(郑州)律师事务所"]
    dtime = time.strftime("%Y%m%d", time.localtime())
    #dtime = dateteime.dateteime.strftime(dateteime.dateteime.now(),"%Y%m%d")
    path = str(dtime) + ".csv"
    #path = '20210209.csv'
    #now_day = int(dtime) - 1
    now_day = int(datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(days=1),"%Y%m%d"))
    #yet_day = now_day - 1
    yet_day = int(datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(days=2),"%Y%m%d"))
    df = pd.read_csv("../data/{}".format(path))
    df["总消"] = df["总消费"] - df["凤巢优惠券消费"] - df["原生CPC优惠券消费"] - df["原生CPM优惠券消费"]
    df["信息流"] = df["阿拉丁CPT消费"] + df["原生阿拉丁消费"] + df["原生CPC消费"] + df["原生CPM消费"] + df["百意Feed消费"] + df[
        "百意无线开屏CPC消费"] + df["原生GD消费"] + df["百意FeedGD消费"] - df["原生CPC优惠券消费"] - df["原生CPM优惠券消费"]
    df["搜索消费"] = df["搜索点击消费"] + df["凤巢阿拉丁消费"] - df["凤巢优惠券消费"]
    data = df[["data_flag","客户名称", "总消", "信息流", "搜索消费"]].copy()

    #雅方消费
    yafang = []
    for col in column_1:
        yafang_total = data[(data["客户名称"] == col) & (data["data_flag"] == now_day)][["总消","信息流","搜索消费"]].sum()
        yafang.append([col, yafang_total])
    #print("雅方消费情况：",yafang)

    #盈科消费减半
    for col in column_4:
        data.loc[data["客户名称"] == col, "总消"] = (data[data["客户名称"] == col]["总消"]) / 2
        data.loc[data["客户名称"] == col, "信息流"] = (data[data["客户名称"] == col]["信息流"]) / 2
        data.loc[data["客户名称"] == col, "搜索消费"] = (data[data["客户名称"] == col]["搜索消费"]) / 2


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
    newQ = pd.merge(newcolum,data_finally,how='left',left_on='公司名称',right_on='客户名称').sort_values(by='消费环比',ascending=False)
    #print(newQ)
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

    #消费大于5000客户
    all_five = data_a[(data_a["消费环比"] >= 3000) | (data_a["消费环比"] <= -3000)].sort_values(by='消费环比',ascending=False)

    top_up = top_cil_up[top_cil_up['消费环比']>2000].sort_values(by='消费环比',ascending=False)
    top_down = top_cil_down[top_cil_down['消费环比']<-2000].sort_values(by='消费环比',ascending=False)
    newD = newQ[(newQ['昨日总消']>=1000) | (newQ['前日总消']>=1000)]

    dataF = textData(total,ratio,top_up,top_down,newD)

    dataFive = all_five_data(total, ratio, top_up, top_down, all_five)


    return total,ratio,top_cil_up,top_cil_down,yafang,newQ,xinkai,dataF,dataFive
def ratioIncrease(data):
    if data>0:
        textStr = "环比增长{}万；".format(np.round(data/10000, 1))
    else:
        textStr = "环比下降{}万；".format(np.round(abs(data/10000),1))
    return textStr

def textData(total,ratio,top_cil_ip,top_down,newQ):
    '''
    data_str = "昨日总消费{}万,{}".format(np.round(total['总消']/10000,1),ratioIncrease(ratio['总消']))\
               +"信息流{}万,{}".format(np.round(total['信息流']/10000,1),ratioIncrease(ratio['信息流']))\
               +"搜索{}万,{}".format(np.round(total['搜索消费']/10000,1),ratioIncrease(ratio['搜索消费']))

    dataS = '昨日主要增长下降客户：\n'
    for i in range(top_cil_ip.shape[0]):
        dataS += '{}{}'.format(top_cil_ip.iloc[i,0],ratioIncrease(top_cil_ip.iloc[i,3]))

    for i in range(top_down.shape[0]):
        dataS += '{}{}'.format(top_down.iloc[i,0],ratioIncrease(top_down.iloc[i,3]))
    '''

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

def all_five_data(total,ratio,top_cil_ip,top_down,all_five):
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

    dataFinall = data_str + '\n' + dataN
    return dataFinall

def main():
    '''
    print(pd.to_datetime('2021/3/10'))
    print(datetime.datetime.now())

    print(pd.PeriodIndex(pd.to_datetime('2021/3/10'),freq='Q'))
    '''
    dataHandle()

    data1,data2 = emailData()
    #sendMessage1 = AutSendEmail(data1,["pinpaiyunying@ruizhiqi.com"])
    sendMessage2 = AutSendEmail(data2,["zhangyanqing@ruizhiqi.com"])

    #sendMessage1.sendMessage()
    sendMessage2.sendMessage()



if __name__ == "__main__":
    main()