  void updateJsonNode(String encType, JsonNode jsonNode) {
    List<String> sensitiveKeys = new ArrayList<>();
    String[] defaultSensitiveFieldsList = kafkaConnectorSensitiveFields.split(",");
    jsonNode
        .fieldNames()
        .forEachRemaining(
            key -> {
              for (String sensitiveField : defaultSensitiveFieldsList) {
                if (key.toLowerCase().contains(sensitiveField.toLowerCase())) {
                  sensitiveKeys.add(key);
                }
              }
            });
    try {
      BasicTextEncryptor jasyptEncryptor = commonUtilsService.getJasyptEncryptor();
      for (String sensitiveKey : sensitiveKeys) {
        if (encType.equals("decrypt")) {
          ((ObjectNode) jsonNode)
              .put(sensitiveKey, jasyptEncryptor.decrypt(jsonNode.get(sensitiveKey).asText()));
        } else if (encType.equals("encrypt")) {
          {
            ((ObjectNode) jsonNode)
                .put(sensitiveKey, jasyptEncryptor.encrypt(jsonNode.get(sensitiveKey).asText()));
          }
        }
      }
    } catch (Exception e) {
      // existing requests where passwords are not encrypted
      log.error("Ignore errors during decryption");
    }
  }

  @PermissionAllowed(permissionAllowed = {PermissionType.APPROVE_CONNECTORS})
  public ApiResponse approveConnectorRequests(String connectorId)
      throws KlawException, KlawRestException {
    log.info("approveConnectorRequests {}", connectorId);
    String userDetails = getUserName();
    int tenantId = commonUtilsService.getTenantId(getUserName());

    if (commonUtilsService.isNotAuthorizedUser(
        commonUtilsService.getPrincipal(), PermissionType.APPROVE_CONNECTORS)) {
      return ApiResponse.NOT_AUTHORIZED;
    }

    KafkaConnectorRequest connectorRequest =
        manageDatabase
            .getHandleDbRequests()
            .getConnectorRequestsForConnector(Integer.parseInt(connectorId), tenantId);

    String jsonConnectorConfig;
    try {
      jsonConnectorConfig =
          createConnectorConfig(connectorRequest, connectorRequest.getRequestOperationType());
    } catch (Exception e) {
      log.error("Exception:", e);
      throw new KlawException(KAFKA_CONNECT_ERR_112);
    }

    if (connectorRequest.getRequestor().equals(userDetails)) {
      return ApiResponse.notOk(KAFKA_CONNECT_ERR_113);
