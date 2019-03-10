FROM java:8-jre

CMD ["java", "-Xmx200m", "-jar", "/app/hadoop_service_eurekay.jar"]

HEALTHCHECK --interval=30s --timeout=30s CMD curl -f http://localhost:8761/actuator/health || exit 1

EXPOSE 8761