
# map-ephys dockerfile

FROM map-ephys:latest

USER root

RUN wget -q -O /root/globusconnectpersonal-latest.tgz \
	https://downloads.globus.org/globus-connect-personal/linux/stable/globusconnectpersonal-latest.tgz \
	&& echo \
		812b0f703bb94156bc50e62dac4d1f03d29612946805cc7e39858ba6e6d5e2d0 \
		/root/globusconnectpersonal-latest.tgz |sha256sum -c \
	&& tar -C /opt -zxf /root/globusconnectpersonal-latest.tgz \
	&& ln -sf /opt/globusconnectpersonal-3.1.1/globusconnectpersonal /usr/local/bin/globusconnectpersonal

USER map

ENTRYPOINT /bin/sh
