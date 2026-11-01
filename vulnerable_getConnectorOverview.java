  public ConnectorOverview getConnectorOverview(String connectorNamesearch, String envId) {
    log.debug("getConnectorOverview {}", connectorNamesearch);
    String userName = getUserName();
    HandleDbRequests handleDb = manageDatabase.getHandleDbRequests();

    int tenantId = commonUtilsService.getTenantId(getUserName());

    if (connectorNamesearch != null) {
      connectorNamesearch = connectorNamesearch.trim();
    } else {
      return null;
    }

    Integer loggedInUserTeam = commonUtilsService.getTeamId(userName);

    List<KwKafkaConnector> connectors = handleDb.getConnectors(connectorNamesearch, tenantId);

    // tenant filtering
    final Set<String> allowedEnvIdSet = commonUtilsService.getEnvsFromUserId(userName);

    connectors =
        connectors.stream()
            .filter(connectorObj -> allowedEnvIdSet.contains(connectorObj.getEnvironment()))
            .collect(Collectors.toList());

    ConnectorOverview connectorOverview = filterByEnvironment(connectors, envId, tenantId);

    if (connectors.size() == 0) {
      connectorOverview.setConnectorExists(false);
      return connectorOverview;
    } else {
      connectorOverview.setConnectorExists(true);
    }

    String syncCluster;
    String[] reqconnsEnvs;
    ArrayList<String> reqconnsEnvsList = new ArrayList<>();

    try {
      syncCluster = manageDatabase.getTenantConfig().get(tenantId).getBaseSyncKafkaConnectCluster();
    } catch (Exception e) {
      log.error("Exception", e);
      syncCluster = null;
    }

    try {
      String requestconnsEnvs =
          commonUtilsService.getEnvProperty(tenantId, "REQUEST_CONNECTORS_OF_KAFKA_CONNECT_ENVS");
      reqconnsEnvs = requestconnsEnvs.split(",");
      reqconnsEnvsList = new ArrayList<>(Arrays.asList(reqconnsEnvs));
    } catch (Exception e) {
      log.error("Error in getting req Connector envs", e);
    }

    List<KafkaConnectorModelResponse> connectorInfoList = new ArrayList<>();
    ArrayList<ResourceHistory> connectorHistory;
    List<ResourceHistory> connectorHistoryList = new ArrayList<>();

    for (KwKafkaConnector conn : connectors) {
      if (StringUtils.isEmpty(envId) || conn.getEnvironment().equals(envId)) {
        KafkaConnectorModelResponse connectorInfo = new KafkaConnectorModelResponse();
        connectorInfo.setConnectorId(conn.getConnectorId());

        connectorInfo.setConnectorName(conn.getConnectorName());
        connectorInfo.setEnvironmentName(
            getKafkaConnectEnvDetails(conn.getEnvironment()).getName());
        connectorInfo.setEnvironmentId(conn.getEnvironment());
        connectorInfo.setConnectorConfig(conn.getConnectorConfig());
        connectorInfo.setTeamName(manageDatabase.getTeamNameFromTeamId(tenantId, conn.getTeamId()));
        connectorInfo.setTeamId(conn.getTeamId());
        // set booleans
        connectorInfo.setHasOpenClaimRequest(
            isClaimRequestOpen(tenantId, connectorInfo.getConnectorName()));
        connectorInfo.setHasOpenRequest(
            connectorInfo.isHasOpenClaimRequest()
                || isConnectorRequestOpen(
                    tenantId, connectorInfo.getConnectorName(), connectorInfo.getEnvironmentId()));
        connectorInfo.setHighestEnv(
            checkIsHighestEnv(
                connectorInfo.getEnvironmentId(), connectorOverview.getAvailableEnvironments()));
        connectorInfo.setConnectorOwner(
            Objects.equals(connectorInfo.getTeamId(), loggedInUserTeam));
        connectorInfo.setShowDeleteConnector(
            connectorInfo.isConnectorOwner()
                && connectorInfo.isHighestEnv()
                && !connectorInfo.isHasOpenRequest());

        connectorInfo.setHasOpenRequestOnAnyEnv(
            isConnectorRequestOpen(tenantId, conn.getConnectorName()));

        if (Objects.equals(syncCluster, conn.getEnvironment())) {
          connectorOverview.setConnectorDocumentation(conn.getDocumentation());
          connectorOverview.setConnectorIdForDocumentation(conn.getConnectorId());
        }

        if (conn.getHistory() != null) {
          try {
            connectorHistory = OBJECT_MAPPER.readValue(conn.getHistory(), VALUE_TYPE_REF);
            connectorHistoryList.addAll(connectorHistory);
          } catch (JsonProcessingException e) {
            log.error("Unable to parse connectorHistory", e);
          }
        }

        connectorInfoList.add(connectorInfo);
      }
    }

    if (connectorOverview.getConnectorIdForDocumentation() == null) {
      connectorOverview.setConnectorDocumentation(connectors.get(0).getDocumentation());
      connectorOverview.setConnectorIdForDocumentation(connectors.get(0).getConnectorId());
    }

    connectorOverview.setConnectorHistoryList(connectorHistoryList);
    // TODO is this needed can we just grab this from the connectors above? circa line 1019
    List<KwKafkaConnector> connectorsSearchList =
        manageDatabase.getHandleDbRequests().getConnectorsFromName(connectorNamesearch, tenantId);

    // tenant filtering
    Integer connectorOwnerTeam =
        getFilteredConnectorsForTenant(connectorsSearchList).get(0).getTeamId();

    for (KafkaConnectorModelResponse connectorInfo : connectorInfoList) {
      // show edit button only for restricted envs
      if (Objects.equals(connectorOwnerTeam, loggedInUserTeam)
          && reqconnsEnvsList.contains(connectorInfo.getEnvironmentId())) {
        connectorInfo.setShowEditConnector(true);
        connectorInfo.setConnectorOwner(true);
      }
    }

    connectorOverview.setConnectorInfoList(connectorInfoList);
    try {
      if (Objects.equals(connectorOwnerTeam, loggedInUserTeam)) {
        connectorOverview.setPromotionDetails(
            getConnectorPromotionEnv(connectorNamesearch, connectors, tenantId));

        if (connectorOverview.getPromotionDetails().getStatus().equals(PromotionStatusType.SUCCESS)
            && !StringUtils.isEmpty(envId)) {
          if (!connectorOverview.getPromotionDetails().getSourceEnv().equals(envId)) {
            connectorOverview.getPromotionDetails().setStatus(PromotionStatusType.NO_PROMOTION);
          }
        }

        if (isPromoteRequestOpen(
            tenantId,
            connectorNamesearch,
            connectorOverview.getPromotionDetails().getTargetEnvId())) {
          connectorOverview.getPromotionDetails().setStatus(PromotionStatusType.REQUEST_OPEN);
        }

        if (connectorInfoList.size() > 0) {
          KafkaConnectorModelResponse lastItem =
              connectorInfoList.get(connectorInfoList.size() - 1);
          lastItem.setConnectorDeletable(true);
        }

      } else {
        notAuthorizedPromotionDetails(connectorOverview);
      }
