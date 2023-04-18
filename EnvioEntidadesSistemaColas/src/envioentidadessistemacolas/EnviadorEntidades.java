package envioentidadessistemacolas;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class EnviadorEntidades {

    public static final String QUEUE_NAME = "entity_queue";

    public static void main(String[] args) throws IOException {
        ConnectionFactory connectionF = new ConnectionFactory();
        connectionF.setHost("localhost");

        try {
            Connection conexion = connectionF.newConnection();
            Channel chan = conexion.createChannel();
            chan.queueDeclare(QUEUE_NAME, false, false, false, null);

            //Para recibir las entidades (usuarios)
            ObjectMapper objectMapper = new ObjectMapper();
            chan.basicConsume(QUEUE_NAME, true, new DefaultConsumer(chan) {
                public void handleDelivry(String consumerTag, Envelope env, AMQP.BasicProperties prop, byte[] body) throws IOException {
                    String msg = new String(body, "UTF-8");
                    Receptorentidades.Entidad entidad = objectMapper.readValue(msg, Receptorentidades.Entidad.class);
                    System.out.println("La entidad: " + entidad + ", fue recibida con éxito.");
                }
            });
            //Enviando entidades
            int i = 0;
            while (true) {
                Receptorentidades.Entidad entidad = new Receptorentidades.Entidad("Enviador", i, 20 + i);
                ObjectMapper mapeador = new ObjectMapper();
                String msg2 = mapeador.writeValueAsString(entidad);
                chan.basicPublish("", QUEUE_NAME, null, msg2.getBytes());
                System.out.println("La entidad: " + entidad + " fue enviada.");
                Thread.sleep(5000);
                i++;
            }
        } catch (TimeoutException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static class Entidad {

        private String name;
        private int id;
        private int valor;

        /**
         * *
         * Constructor por omisión
         */
        public Entidad() {

        }

        public Entidad(String name, int id, int valor) {
            this.name = name;
            this.id = id;
            this.valor = valor;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getValor() {
            return valor;
        }

        public void setValor(int valor) {
            this.valor = valor;
        }

        @Override
        public String toString() {
            return "Entidad{ " + "name=" + name + ", id=" + id + ", valor=" + valor + '}';
        }

    }
}
