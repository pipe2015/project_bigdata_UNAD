#///////////////////////////////////////////////**********************Main Hadoop config (main_hadoop.sh)**************************************/////////////////

#!/bin/bash

# Definir colores
YELLOW_BACKGROUND="\e[43m"
BLUE_TEXT="\e[34m"
RED_BACKGROUND="\e[41m"
WHITE_TEXT="\e[97m"
RESET="\e[0m"

# no se puede usar:: hay que ejecutarlo usando la terminal putty
: <<'END'
# Actualización del sistema
echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Actualizando el sistema${RESET}"
sudo apt-get update -y
sudo apt-get upgrade -y
sudo apt-get dist-upgrade -y

# Reboot del sistema (si es necesario)
echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Preferencias de reinicio del sistema:\nActualiza la Red -> preferencias -> Adaptador Puente${RESET}"
read -p "¿Deseas reiniciar ahora? (s/n): " reboot_choice
if [[ "$reboot_choice" == "s" ]]; then
  sudo reboot
fi
END

# Instalar herramientas necesarias
echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Instalando dependencias${RESET}"
sudo apt install wget apt-transport-https gnupg2 software-properties-common -y
sudo apt update
sudo apt install openjdk-11-jdk -y

# Verificar la versión de Java
echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Verificando instalación de Java${RESET}"
java -version || { echo -e "${RED_BACKGROUND}${WHITE_TEXT}Error: No se pudo verificar la instalación de Java.${RESET}"; exit 1; }

# Instalar OpenSSH
echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Instalando OpenSSH${RESET}"
sudo apt install openssh-server openssh-client -y

# Crear usuario hadoop
echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Creando usuario hadoop${RESET}"
sudo adduser hadoop
echo -e "${YELLOW_BACKGROUND}${BLUE_TEXT}Cambiando de usuario -> hadoop${RESET}"
sudo su - hadoop
