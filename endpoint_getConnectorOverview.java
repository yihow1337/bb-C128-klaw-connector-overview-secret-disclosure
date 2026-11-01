        HttpStatus.OK);
  }

  @PermissionAllowed(permissionAllowed = {PermissionType.VIEW_CONNECTORS})
  @RequestMapping(
      value = "/getConnectorOverview",
      method = RequestMethod.GET,
      produces = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<ConnectorOverview> getConnectorOverview(
      @RequestParam(value = "connectornamesearch") String connectorNameSearch,
      @RequestParam(value = "environmentId", defaultValue = "") String environmentId) {
    return new ResponseEntity<>(
        kafkaConnectControllerService.getConnectorOverview(connectorNameSearch, environmentId),
        HttpStatus.OK);
  }

  @PostMapping(
      value = "/createClaimConnectorRequest",
      produces = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<ApiResponse> createClaimConnectorRequest(
