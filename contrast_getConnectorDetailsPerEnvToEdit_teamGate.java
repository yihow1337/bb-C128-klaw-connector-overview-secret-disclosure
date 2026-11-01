  public ConnectorOverviewPerEnv getConnectorDetailsPerEnvToEdit(String envId, String connectorName)
      throws KlawBadRequestException {
    ConnectorOverviewPerEnv connectorOverviewPerEnv = new ConnectorOverviewPerEnv();
    connectorOverviewPerEnv.setConnectorExists(false);
    String userName = getUserName();
    int tenantId = commonUtilsService.getTenantId(userName);

    KafkaConnectorModelResponse connectorModel = new KafkaConnectorModelResponse();
    List<KwKafkaConnector> connectors =
        manageDatabase.getHandleDbRequests().getConnectors(connectorName, tenantId);

    // tenant filtering
    final Set<String> allowedEnvIdSet = commonUtilsService.getEnvsFromUserId(userName);
    connectors =
        connectors.stream()
            .filter(topicObj -> allowedEnvIdSet.contains(topicObj.getEnvironment()))
            .collect(Collectors.toList());

    String topicDescription = "";
    if (connectors.size() == 0) {
      connectorOverviewPerEnv.setError("Connector does not exist.");
      throw new KlawBadRequestException(
          String.format("Connector %s does not exist.", connectorName));
    } else {
      Optional<KwKafkaConnector> topicDescFound =
          connectors.stream()
              .filter(
                  topic -> topic.getDescription() != null && topic.getDescription().length() > 0)
              .findFirst();
      if (topicDescFound.isPresent()) {
        topicDescription = topicDescFound.get().getDescription();
      }
      connectors =
          connectors.stream()
              .filter(topic -> Objects.equals(topic.getEnvironment(), envId))
              .collect(Collectors.toList());
    }

    String loggedInUserTeam =
        manageDatabase
            .getHandleDbRequests()
            .getAllTeamsOfUsers(getUserName(), tenantId)
            .get(0)
            .getTeamname();

    for (KwKafkaConnector kafkaConnector : connectors) {
      connectorModel.setEnvironmentName(
          getKafkaConnectEnvDetails(kafkaConnector.getEnvironment()).getName());
      connectorModel.setEnvironmentId(kafkaConnector.getEnvironment());

      connectorModel.setTeamName(
          manageDatabase.getTeamNameFromTeamId(tenantId, kafkaConnector.getTeamId()));
      if (!Objects.equals(loggedInUserTeam, connectorModel.getTeamName())) {
        throw new KlawBadRequestException(KAFKA_CONNECT_ERR_119);
      }
      connectorOverviewPerEnv.setConnectorId(kafkaConnector.getConnectorId());

      JsonNode jsonNode;
      try {
        jsonNode = OBJECT_MAPPER.readTree(kafkaConnector.getConnectorConfig());
        updateJsonNode("decrypt", jsonNode);
        ObjectNode objectNode = (ObjectNode) jsonNode;
        connectorModel.setConnectorConfig(
            WRITER_WITH_DEFAULT_PRETTY_PRINTER.writeValueAsString(objectNode));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    connectorModel.setDescription(topicDescription);

    if (connectorModel.getConnectorConfig() != null) {
      connectorOverviewPerEnv.setConnectorExists(true);
      connectorOverviewPerEnv.setConnectorContents(connectorModel);
    }
    return connectorOverviewPerEnv;
  }

  private ConnectorPromotionStatus getConnectorPromotionEnv(
      String topicSearch, List<KwKafkaConnector> kafkaConnectors, int tenantId) {
    ConnectorPromotionStatus promotionStatus = new ConnectorPromotionStatus();
    try {
      if (kafkaConnectors == null)
        kafkaConnectors = manageDatabase.getHandleDbRequests().getConnectors(topicSearch, tenantId);

      promotionStatus.setConnectorName(topicSearch);

      if (kafkaConnectors != null && kafkaConnectors.size() > 0) {
        List<String> envList = new ArrayList<>();
        kafkaConnectors.forEach(topic -> envList.add(topic.getEnvironment()));

        // tenant filtering
        String orderOfEnvs =
            commonUtilsService.getEnvProperty(tenantId, "ORDER_OF_KAFKA_CONNECT_ENVS");
        if (orderOfEnvs.length() == 0) {
          // No promotion order set return no promotion
          promotionStatus.setStatus(PromotionStatusType.NO_PROMOTION);
          return promotionStatus;
        }

        envList.sort(Comparator.comparingInt(orderOfEnvs::indexOf));

        String lastEnv = envList.get(envList.size() - 1);
        AtomicReference<String> sourceConnectorConfig = new AtomicReference<>("");
        kafkaConnectors.stream()
            .filter(kwKafkaConnector -> Objects.equals(kwKafkaConnector.getEnvironment(), lastEnv))
            .findFirst()
            .ifPresent(a -> sourceConnectorConfig.set(a.getConnectorConfig()));

        List<String> orderdEnvs = Arrays.asList(orderOfEnvs.split(","));

        if (orderdEnvs.indexOf(lastEnv) == orderdEnvs.size() - 1) {
          promotionStatus.setStatus(PromotionStatusType.NO_PROMOTION); // PRD
        } else {
          promotionStatus.setStatus(PromotionStatusType.SUCCESS);
          promotionStatus.setSourceEnv(lastEnv);
          promotionStatus.setSourceConnectorConfig(sourceConnectorConfig.get());
          String targetEnv = orderdEnvs.get(orderdEnvs.indexOf(lastEnv) + 1);
          if (getKafkaConnectEnvDetails(targetEnv) != null) {
            promotionStatus.setTargetEnv(getKafkaConnectEnvDetails(targetEnv).getName());
