## Future Improvements
- [ ] Exploratory notebook
- [ ] Infrastructure as Code with Terraform (BigQuery dataset, IAM)
- [ ] Data Lake in Google Cloud Storage (raw and processed layers)
- [ ] Orchestration (e.g. Kestra / Airflow)


services:
  kestra:
    image: kestra/kestra:latest
    container_name: kestra
    command: server local
    ports:
      - "8080:8080"
    volumes:
      - ./:/app/project
    env_file:
    - .env.kestra
    environment:
      KESTRA_CONFIGURATION: |
          datasources:
            kestra:
              server:
                basic-auth:
                  username: ${KESTRA_USERNAME}
                  password: ${KESTRA_PASSWORD}
      RAWG_API_KEY: ${RAWG_API_KEY}
      GCS_BUCKET: ${GCS_BUCKET}
      GCS_RAW_PREFIX: ${GCS_RAW_PREFIX}
      GOOGLE_APPLICATION_CREDENTIALS: ${GOOGLE_APPLICATION_CREDENTIALS}