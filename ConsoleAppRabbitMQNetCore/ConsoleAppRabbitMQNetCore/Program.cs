using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace ConsoleAppRabbitMQNetCore
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("RabbitMQ，Hello World!");
            Console.WriteLine("当type等于1时，客户端为生产者，2时为消费者.");
            string type = Console.ReadLine();
            //生产者
            if(type == "1")
            {
                ConnectionFactory factory = new ConnectionFactory();
                factory.HostName = "127.0.0.1";
                //默认端口5672
                factory.Port = 5672;
                using (IConnection conn = factory.CreateConnection())
                {
                    using (IModel channel = conn.CreateModel())
                    {
                        //在MQ上定义一个持久化队列，如果名称相同不会重复创建
                        channel.QueueDeclare("MyRabbitMQ2018102401", true, false, false, null);
                        while(true)
                        {
                            string message = string.Format("Message_{0}", Console.ReadLine());
                            byte[] buffer = Encoding.UTF8.GetBytes(message);
                            IBasicProperties basicProperties = channel.CreateBasicProperties();
                            basicProperties.DeliveryMode = 2;
                            channel.BasicPublish("", "MyRabbitMQ2018102401", basicProperties, buffer);
                            Console.WriteLine("消息发送成功：" + message);
                        }
                    }
                }

            }
            else
            {
                //消费者
                ConnectionFactory factory = new ConnectionFactory();
                factory.HostName = "127.0.0.1";
                //默认端口
                factory.Port = 5672;
                using (IConnection conn = factory.CreateConnection())
                {
                    using (IModel channel = conn.CreateModel())
                    {
                        //在MQ上定义一个持久化队列，如果名称相同不会重复创建
                        channel.QueueDeclare("MyRabbitMQ2018102401", true, false, false, null);
                        //输入1，那如果接收一个消息，但是没有应答，则客户端不会收到下一个消息
                        channel.BasicQos(0, 1, false);
                        Console.WriteLine("Listening...");
                        //在队列上定义一个消费者
                        var consumer = new EventingBasicConsumer(channel);
                        //消费队列，并设置应答模式为程序主动应答
                        channel.BasicConsume("MyRabbitMQ2018102401", false, consumer);
                        //阻塞函数，获取队列中的消息
                        consumer.Received += (ch, ea) =>
                        {
                            byte[] bytes = ea.Body;
                            string str = Encoding.UTF8.GetString(bytes);
                            Console.WriteLine("队列消息:" + str.ToString());
                            //回复确认
                            channel.BasicAck(ea.DeliveryTag, false);
                        };
                        Console.WriteLine("按任意值，退出程序");
                        Console.ReadKey();
                    }
                }


            }
        }
    }
}
