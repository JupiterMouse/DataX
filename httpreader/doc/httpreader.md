# DataX HttpReader 插件文档

------------

## 1 快速介绍

HttpWriter提供读取HTTP数据的能力，支持读取SOAP、JSON格式的数据，并转换为DataX传输协议传递给Writer。


## 2 功能与限制

* 支持SOAP、JSON格式的数据读取。
* 认证方式可以扩展，目前支持BASIC认证。

暂时不能做到：

* 分页读取数据

## 3 功能说明


### 3.1 配置样例

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": "3"
      },
      "errorLimit": {
        "record": "0",
        "percentage": "0"
      }
    },
    "content": [
      {
        "reader": {
          "name": "httpreader",
          "parameter": {
            "url": "http://xxx:8068/sap/bc/srt/rfc/sap/zhri_hrbi_auth/800/zhri_hrbi_auth_800/zhri_hrbi_auth_800",
            "headers": {
              "Content-Type": "text/xml;charset=UTF-8",
              "SOAPAction": "urn:sap-com:document:sap:rfc:functions:ZHRI_HRBI_AUTH:ZHRI_HRBI_AUTHRequest"
            },
            "auth": {
              "type": "BASIC",
              "username": "xxx",
              "password": "xxx"
            },
            "format": "SOAP",
            "method": "POST",
            "payload": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:urn=\"urn:sap-com:document:sap:rfc:functions\">\n   <soapenv:Header/>\n   <soapenv:Body>\n      <urn:ZHRI_HRBI_AUTH>\n         <ET_AUTHLIST>\n            <item>\n            </item>\n         </ET_AUTHLIST>\n         <IV_LEAVE>X</IV_LEAVE>\n      </urn:ZHRI_HRBI_AUTH>\n   </soapenv:Body>\n</soapenv:Envelope>",
            "mapPath": "soap-env:Envelope,soap-env:Body,n0:ZHRI_HRBI_AUTHResponse,ET_AUTHLIST,item",
            "column": [
              "PERNR",
              "SUB_PERNR"
            ]
          }
        },
        "writer": {
          "name": "postgresqlwriter",
          "parameter": {
            "schema": "hr_ods",
            "username": "xxx",
            "password": "xxx",
            "column": [
              "pernr",
              "sub_pernr"
            ],
            "connection": [
              {
                "table": [
                  "hr_ods.hr_sap_et_authlist"
                ],
                "jdbcUrl": "jdbc:postgresql://xxx:5432/hr"
              }
            ],
            "preSql": [
              "truncate table hr_ods.hr_sap_et_authlist;"
            ],
            "postSql": [
              ""
            ],
            "batchSize": 1024
          }
        }
      }
    ]
  }
}
```

### 3.2 参数说明

* **url**

	* 描述：HTTP请求的URL路径。格式：http://ip:端口/路径；例如：http://hanas1.hand-china.com:8068/sap/bc/srt/rfc/sap/zhri_hrbi_auth/800/zhri_hrbi_auth_800/zhri_hrbi_auth_800

	* 必选：是 <br />

	* 默认值：无 <br />

* **headers**

	* 描述：HTTP的请求头

		数据格式为SOAP时，必包含`Content-Type:text/xml;charset=UTF-8`

		数据格式为JSON时，必包含`Content-Type:application/json;charset=UTF-8`

	* 必选：是 

	* 默认值：无 
	
* **auth**

  * 描述：请求的认证方式及参数

    BASCI认证时，必包含`username`、`password`

  * 必选：否

  * 默认值：无 

* **format**

     描述：数据传输格式。 

     SOAP｜JSON

     * 必选：是 

     * 默认值：无

* **method**

     描述：HTTP请求的方法。 

     POST｜GET

     * 必选：是 

     * 默认值：无

* **payload**

     描述：HTTP请求的数据。

     * 必选：无

     * 默认值：无

* **mapPath**

     描述：返回数据的树寻址路径。

     * 必选：是
     * 默认值：无

* **column**

  * 描述：读取字段列表，name指定字段名，type指定源数据的类型。

    用户可以指定Column字段信息，配置如下：

    对于用户指定Column信息，name必须填写，type为空默认取string。

    ```json
        "column": [
          {
            "name": "PERNR",  -- 字段名
            "type": "long"  -- 字段类型
          },
          {
            "name": "ENAME.l1.l2.l3",
            "type": "string"
          },
          {
            "name": "GESCH",
            "type": "string"
          },
          {
            "name": "ORGEH",
            "type": "string"
          },
          {
            "name": "ENDDATE",
            "type": "date",       -- date类型
            "format": "yyyy-MM-dd" -- 事件格式
          }
        ]
    ```

    极简模式

    用户安装目标端字段顺序指定字段来源，默认类型为string，默认值为NULL

    ```json
                "column": [
                  "ORG_L1",
                  "ORG_L2",
                  "ORG_L3",
                  "ORG_L4"
                ]
    ```

* 3.3 类型转换


本地文件本身不提供数据类型，该类型是DataX HttpReader定义：

| DataX 内部类型| 本地文件 数据类型    |
| -------- | -----  |
|
| Long     |Long |
| Double   |Double|
| String   |String|
| Boolean  |Boolean |
| Date     |Date |

其中：

* 接口数据 Long是指接口返回文本中使用整形或整形的字符串表示形式，例如2020，"19901219"。
* 接口数据 Double是指接口返回文本中使用Double的字符串表示形式，例如"3.1415"。
* 接口数据 Boolean是指接口返回文本中使用Boolean的字符串表示形式，例如"true"、"false"。不区分大小写。
* 接口数据 Date是指接口返回文本中使用Date的字符串表示形式，例如"2014-12-31"，Date可以指定format格式。


## 4 性能报告




## 5 配置步骤：
略

## 6 约束限制

略

## 6 FAQ

略
