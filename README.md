**maxwell读取mysql日志发送给kafka**

**kafka安装：**

http://kafka.apache.org/

**maxwell下载安装：

http://maxwells-daemon.io
请认真阅读http://maxwells-daemon.io/quickstart/ 参考mysql配置

**建议使用docker技术**

    ##docker容器启动命令：
    docker run -it --rm zendesk/maxwell bin/maxwell --user=$MYSQL_USERNAME \
        --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=kafka \
        --kafka.bootstrap.servers=$KAFKA_HOST:$KAFKA_PORT --kafka_topic=maxwell
     
    ##注意：
    1、由于容器技术$KAFKA_HOST和$MYSQL_HOST不能写locahost或者127.0.0.1;
    2、$MYSQL_USERNAME和$MYSQL_PASSWORD可以根据需要不必建立maxwell用户
    3、--kafka_topic=maxwell是kafka的topic为maxwell，根据实际需要
    4、此监控只是前台，如果要转如后台运行，改"-it"为"-d"。
    
    以下是我修改结果：
    docker run -d --rm zendesk/maxwell bin/maxwell --user=root \
    --password=123456 --host=192.168.22.129 --producer=kafka \
    --kafka.bootstrap.servers=192.168.22.129:9092 --kafka_topic=test
    
    
**项目：

    @KafkaListener(topics = "test")
    public void listenT2(ConsumerRecord<?, ?> cr) throws Exception {
        String mysql = Maxwell2MysqlUtil.maxwell2Mysql(cr);
        System.out.println("sql:"+mysql);
    }
    
    注意：topics = "test"修改为maxwell存放的topic名称
    

