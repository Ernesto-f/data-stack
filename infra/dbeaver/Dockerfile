FROM dbeaver/cloudbeaver:24.3.0

LABEL maintainer="Felipe Eidt"
LABEL description="CloudBeaver - Apex Data Platform"

ENV JAVA_OPTS="-Xms512m -Xmx2g"
ENV CLOUDBEAVER_ROOT_URI=/dbeaver/

# workspace
RUN mkdir -p /opt/cloudbeaver/workspace

EXPOSE 8978

CMD ["/opt/cloudbeaver/run-server.sh"]