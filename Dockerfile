FROM debian:latest
RUN echo "deb http://mirrors.aliyun.com/debian/ buster main contrib non-free\n\
deb http://mirrors.aliyun.com/debian/ buster-updates main contrib non-free\n\
deb http://mirrors.aliyun.com/debian/ buster-backports main contrib non-free\n\
deb http://mirrors.aliyun.com/debian-security buster/updates main contrib non-free\n" > /etc/apt/sources.list && apt-get update && apt-get install -y git && apt-get install -y make && apt-get install -y golang-1.14 && ln -s /usr/lib/go-1.14/bin/go /usr/bin/go && mkdir -p /root/go/src/
COPY ./ /root/go/src/medispatcher/
CMD cd /root/go/src/medispatcher/ && make
