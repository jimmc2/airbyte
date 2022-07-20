import React, { useState } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { FormPageContent } from "components/ConnectorBlocks";
import HeadTitle from "components/HeadTitle";
import PageTitle from "components/PageTitle";

import { ConnectionConfiguration } from "core/domain/connection";
import { useCreateDestination } from "hooks/services/useDestinationHook";
import { useDestinationDefinitionList } from "services/connector/DestinationDefinitionService";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout";

import { DestinationForm } from "./components/DestinationForm";

export const CreateDestinationPage: React.FC = () => {
  const navigate = useNavigate();
  const [successRequest, setSuccessRequest] = useState(false);

  const { destinationDefinitions } = useDestinationDefinitionList();
  const { mutateAsync: createDestination } = useCreateDestination();

  const onSubmitDestinationForm = async (values: {
    name: string;
    serviceType: string;
    connectionConfiguration?: ConnectionConfiguration;
  }) => {
    const connector = destinationDefinitions.find((item) => item.destinationDefinitionId === values.serviceType);
    const result = await createDestination({
      values,
      destinationConnector: connector,
    });
    setSuccessRequest(true);
    setTimeout(() => {
      setSuccessRequest(false);
      navigate(`../${result.destinationId}`);
    }, 2000);
  };

  return (
    <>
      <HeadTitle titles={[{ id: "destinations.newDestinationTitle" }]} />
      <ConnectorDocumentationWrapper>
        <PageTitle title={null} middleTitleBlock={<FormattedMessage id="destinations.newDestinationTitle" />} />
        <FormPageContent>
          <DestinationForm
            onSubmit={onSubmitDestinationForm}
            destinationDefinitions={destinationDefinitions}
            hasSuccess={successRequest}
          />
        </FormPageContent>
      </ConnectorDocumentationWrapper>
    </>
  );
};
