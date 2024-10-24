#///////////////////////////////////////////////**********************Hadoop config (Init_Hadoop.sh)**************************************/////////////////

#!/bin/bash

# Definir colores
YELLOW_BACKGROUND="\e[43m"
BLUE_TEXT="\e[34m"
RED_BACKGROUND="\e[41m"
WHITE_TEXT="\e[97m"
RESET="\e[0m"

# Generar claves SSH para el usuario hadoop
echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Generando claves SSH para hadoop${RESET}"
ssh-keygen -t rsa -f ~/.ssh/id_rsa -q -N ""
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 640 ~/.ssh/authorized_keys

# Prueba SSH
echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Probando SSH en localhost...${RESET}"
ssh -o "StrictHostKeyChecking=no" localhost echo "SSH funciona correctamente."

# Descargar y descomprimir Hadoop
echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Descargando Hadoop...${RESET}"
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xvzf hadoop-3.3.6.tar.gz
mv hadoop-3.3.6 hadoop

# Configurar variables de entorno para Java y Hadoop
echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Configurando entorno de Hadoop y Java...${RESET}"
JAVA_PATH=$(dirname $(dirname $(readlink -f $(which java))))
echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}JAVA_HOME: $JAVA_PATH${RESET}"
echo "export JAVA_HOME=$JAVA_PATH" >> ~/.bashrc
echo "export HADOOP_HOME=~/hadoop/hadoop" >> ~/.bashrc
echo "export HADOOP_INSTALL=\$HADOOP_HOME" >> ~/.bashrc
echo "export HADOOP_MAPRED_HOME=\$HADOOP_HOME" >> ~/.bashrc
echo "export HADOOP_COMMON_HOME=\$HADOOP_HOME" >> ~/.bashrc
echo "export HADOOP_HDFS_HOME=\$HADOOP_HOME" >> ~/.bashrc
echo "export HADOOP_YARN_HOME=\$HADOOP_HOME" >> ~/.bashrc
echo "export HADOOP_COMMON_LIB_NATIVE_DIR=\$HADOOP_HOME/lib/native" >> ~/.bashrc
echo "export PATH=\$PATH:\$HADOOP_HOME/sbin:\$HADOOP_HOME/bin" >> ~/.bashrc
echo "export HADOOP_OPTS=\"-Djava.library.path=\$HADOOP_HOME/lib/native\"" >> ~/.bashrc

#recargar variables de entorno
source ~/.bashrc

if [ ! -f "$HADOOP_HOME/etc/hadoop/hadoop-env.sh" ]; then
    echo "El archivo $HADOOP_HOME/etc/hadoop/hadoop-env.sh no existe."
    exit 1
fi

# Editar hadoop-env.sh
echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Configurando hadoop-env.sh...${RESET}"
sed -i 's|# export JAVA_HOME=.*|export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64|' "$HADOOP_HOME/etc/hadoop/hadoop-env.sh"

# Crear directorios de datos para Hadoop
echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Creando directorios de datos para HDFS...${RESET}"
mkdir -p ~/hadoopdata/hdfs/{namenode,datanode}

# Incrustar el contenido directamente en core-site.xml
# Configurar core-site.xml
# Verificar si el archivo core-site.xml existe
if [ ! -f "$HADOOP_HOME/etc/hadoop/core-site.xml" ]; then
    echo "El archivo $HADOOP_HOME/etc/hadoop/core-site.xml no existe."
    exit 1
fi

# Insertar el bloque XML después de la primera aparición de <configuration>
echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Configurando core-site.xml...${RESET}"
sed -i '/<configuration>/a \
<property>\n\
    <name>fs.defaultFS</name>\n\
    <value>hdfs://localhost:9000</value>\n\
</property>' "$HADOOP_HOME/etc/hadoop/core-site.xml"

echo "Código insertado en $HADOOP_HOME/etc/hadoop/core-site.xml."

# Verificar si el archivo hdfs-site.xml existe y agregar el bloque correspondiente
if [ ! -f "$HADOOP_HOME/etc/hadoop/hdfs-site.xml" ]; then
    echo "El archivo $HADOOP_HOME/etc/hadoop/hdfs-site.xml no existe."
    exit 1
fi

echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Configurando hdfs-site.xml...${RESET}"
sed -i '/<configuration>/a \
<property>\n\
    <name>dfs.replication</name>\n\
    <value>1</value>\n\
</property>\n\
<property>\n\
    <name>dfs.name.dir</name>\n\
    <value>file:///home/hadoop/hadoopdata/hdfs/namenode</value>\n\
</property>\n\
<property>\n\
    <name>dfs.data.dir</name>\n\
    <value>file:///home/hadoop/hadoopdata/hdfs/datanode</value>\n\
</property>' "$HADOOP_HOME/etc/hadoop/hdfs-site.xml"

echo "Código insertado en $HADOOP_HOME/etc/hadoop/hdfs-site.xml."

# Verificar si el archivo mapred-site.xml existe y agregar el bloque correspondiente
if [ ! -f "$HADOOP_HOME/etc/hadoop/mapred-site.xml" ]; then
    echo "El archivo $HADOOP_HOME/etc/hadoop/mapred-site.xml no existe."
    exit 1
fi

echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Configurando mapred-site.xml...${RESET}"
sed -i '/<configuration>/a \
<property>\n\
    <name>mapreduce.framework.name</name>\n\
    <value>yarn</value>\n\
</property>' "$HADOOP_HOME/etc/hadoop/mapred-site.xml"

echo "Código insertado en $HADOOP_HOME/etc/hadoop/mapred-site.xml."

# Verificar si el archivo yarn-site.xml existe y agregar el bloque correspondiente
if [ ! -f "$HADOOP_HOME/etc/hadoop/yarn-site.xml" ]; then
    echo "El archivo $HADOOP_HOME/etc/hadoop/yarn-site.xml no existe."
    exit 1
fi

echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Configurando yarn-site.xml...${RESET}"
sed -i '/<configuration>/a \
<property>\n\
    <name>yarn.nodemanager.aux-services</name>\n\
    <value>mapreduce_shuffle</value>\n\
</property>' "$HADOOP_HOME/etc/hadoop/yarn-site.xml"

echo "Código insertado en $HADOOP_HOME/etc/hadoop/yarn-site.xml."

# Formatear el Namenode
echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Formateando el Namenode...${RESET}"
hdfs namenode -format

# Verificar el código de salida del comando anterior
if [[ $? -eq 0 ]]; then
    echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}El formateo del Namenode fue exitoso.${RESET}"
else
    echo -e "${RED_BACKGROUND}${WHITE_TEXT}Error: El formateo del Namenode falló.${RESET}"
    exit 1  # Salir del script con un código de error
fi

# Preguntar si se desea iniciar Hadoop
echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}¿Deseas iniciar Hadoop ahora?${RESET}"
read -p "Escribe 's' para iniciar o 'n' para salir: " start_hadoop

if [[ "$start_hadoop" == "s" ]]; then
    echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Iniciando Hadoop...${RESET}"
    start-all.sh
else
    echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}El script ha terminado sin iniciar Hadoop.${RESET}"
    exit 0
fi

echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Script de configuración completado.${RESET}"