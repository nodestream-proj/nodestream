# Interpretation Reference

## Normalization Flags

## Source Node Interpretation

| Parameter Name     | Required? | Type                    | Description                                                                                                                                                                                                                                                                                                            |
|--------------------|-----------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| node_type          | Yes       | String or ValueProvider | Specifies the type of the source node. It is a required field. When a  ValueProvider is used dynamic index creation and schema introspection are not supported.                                                                                                                |
| key                | Yes       | Dictionary              | Contains key-value pairs that define the key of the source node.  The keys represent field names, and the values can be either static values or value providers.  It is a required field.                                                                                                                              |
| parameters         | No        | Dictionary              | Stores additional properties of the source node. It is a dictionary  where the keys represent property names, and the values can be either  static values or value providers. This field is optional.                                                                                                                  |
| additional_indexes | No        | List[String]            | Specifies additional indexes for desired on the source node. It is a list of field names. This field is optional.                                                                                                                                                                                                      |
| additional_types   | No        | List[String]            | Defines additional types for the source node. It is a list of strings representing the additional types.  These types are not considered by ingestion system as part of the identity of the node and rather considered as  extra labels applied after the ingestion of the node is completed.  This field is optional. |
| normalization      | No        | Dictionary              | Contains normalization flags that should be adopted by value providers when getting values. This field is optional. See the normalization reference.  By default `do_lowercase_strings` is enabled.                                                                                                                    |

## Relationship Interpretation

## Properties Interpretation

## Extract Variables Interpretation

## Switch Interpretation
