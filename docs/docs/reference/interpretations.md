# Interpretations

## Source Node Interpretation

The source node interpretation is used to define the node that is the conceptual center of the interpretation.
Relationships, and the nodes they connect to, are defined relative to the source node. 
Source nodes are always upserted.
That means that if a node with the same key already exists, it will be updated with the new properties, relationships, and labels.
If a node with the same key does not exist, it will be created.

A source node's uniqueness is defined by the combination of its type and key. 
The `type` represents a label that is applied to the node while the `key` represents the fields that are used to uniquely identify the node relative to other nodes of the same type. 
As an example, if we have two nodes of type `Person` with the same `name` field, they will be considered the same node. 
However, if we have two nodes of type `Person` with different `name` fields, they will be considered different nodes. 
The `key` field is a dictionary where the keys represent field names, and the values can be either static values or [value providers](./value-providers.md).
As an example, if we wanted to define a source node of type `Person` with a `name` field of `John`, we would use the following interpretation:

```yaml
- type: source_node
  node_type: Person
  key:
    name: John
```

Nodestream will automatically create indexes and/or constraints for the node key fields.
As mentioned, you can also use value providers to define the key. 
This is most often what you will do. 
As an example, if we wanted to define a source node of type `Person` with a `name` field where the value of the `name` key is defined by a field called `p_name` in your input JSON, we would use the following interpretation:

```yaml
- type: source_node
  node_type: Person
  key:
    name: !jmespath p_name
```

Beyond the `type` and `key` fields, you can also define additional properties, indexes, and types for the source node.
The example below expands on our previous example by:
* Adding a `properties` field that defines a `birthday` property.
* Adding an `additional_indexes` field that defines an index on the `birthday` property.
* Adding an `additional_types` field that defines a `Patient` type for the source node.

```yaml
- type: source_node
  node_type: Person
  key:
    name: !jmespath patient_name
  properties:
    birthday: !jmespath patient_birthday
  additional_indexes:
    - birthday
  additional_types:
    - Patient
```

Here is a full list of the fields that can be used in a source node interpretation: 

| Parameter Name     | Required? | Type                    | Description                                                                                                                                                                                                                                                                                                            |
|--------------------|-----------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| node_type          | Yes       | String or ValueProvider | Specifies the type of the source node. It is a required field. When a  ValueProvider is used dynamic index creation and schema introspection are not supported.                                                                                                                |
| key                | Yes       | Dictionary              | Contains key-value pairs that define the key of the source node.  The keys represent field names, and the values can be either static values or value providers.  It is a required field.                                                                                                                              |
| properties         | No        | Dictionary              | Stores additional properties of the source node. It is a dictionary  where the keys represent property names, and the values can be either  static values or value providers. This field is optional.                                                                                                                  |
| additional_indexes | No        | List[String]            | Specifies additional indexes for desired on the source node. It is a list of field names. This field is optional.                                                                                                                                                                                                      |
| additional_types   | No        | List[String]            | Defines additional types for the source node. It is a list of strings representing the additional types.  These types are not considered by ingestion system as part of the identity of the node and rather considered as  extra labels applied after the ingestion of the node is completed.  This field is optional. |
| normalization      | No        | Dictionary              | Contains normalization flags that should be adopted by value providers when getting values. This field is optional. [See the normalization reference](./normalizers.md).  By default `do_lowercase_strings` is enabled.                                                                                                                    |

## Relationship Interpretation

### Overview
The relationship interpretation is used to define a relationship between the source node and a related node.
Because there are many different ways to define a relationship, the relationship interpretation is the most complex interpretation.
The relationship interpretation is also the most powerful interpretation.

The most basic relationship interpretation defines a relationship between the source node and a related node. 
The related node is defined by the `node_type` and `node_key` fields while the relationship is defined by the `relationship_type` field.
For example, if we wanted to define a relationship between a source node of type `Person` and a related node of type `Address` with a relationship type of `LIVES_AT`, we would use the following interpretation:

```yaml
- type: relationship
  node_type: Address
  relationship_type: LIVES_AT
  node_key:
    address: !jmespath address
```

The `node_key` field is a dictionary where the keys represent field names, and the values can be either static values or [value providers](./value-providers.md).

The `relationship_type` field is a string or value provider that represents the type of the relationship. 
The `relationship_type` field is a required field. 
When a value provider is used dynamic index creation and schema introspection are not supported.

The `node_type` field is a string or value provider that represents the type of the related node.
The `node_type` field is a required field. 
When a value provider is used dynamic index creation and schema introspection are not supported.

### Directionality

By default, relationships are outbound from the source node. You can change this by setting the `outbound` field to `false`.
For example, if we wanted to define a `HAS_CHILD` relationship between a source node of type `Person` and a related node of type `Person` with the `HAS_CHILD` relationship type, we would use the following interpretation:

```yaml
- type: relationship
  node_type: Person
  relationship_type: HAS_CHILD
  node_key:
    name: !jmespath parent_name
  outbound: false
```

### Relationship Properties 

You can also define properties on the relationship itself. This is done by using the `relationship_properties` field. 
The `relationship_properties` field is a dictionary where the keys represent property names, and the values can be either static values or [value providers](./value-providers.md).

For example, if we wanted to define a `LIVES_AT` relationship between a source node of type `Person` and a related node of type `Address` with a `LIVES_AT` relationship type and a `since` property, we would use the following interpretation:

```yaml
- type: relationship
  node_type: Address
  relationship_type: LIVES_AT
  node_key:
    address: !jmespath address
  relationship_properties:
    since: !jmespath since
```

### Relationship Keys

By default, relationships are unique based on the source node, related node, and relationship type. However, you can define a key for the relationship itself. This is done by using the `relationship_key` field. The `relationship_key` field is a dictionary where the keys represent field names, and the values can be either static values or [value providers](./value-providers.md). This key will be used to uniquely identify the relationship. If a relationship of the same type with the same key already exists between the same nodes, it will be updated with the new properties. If a relationship with the same key does not exist, it will be created.

For example, if we wanted to define a `PLAYED_IN` relationship between a source node of type `Person` and a related node of type `Movie` with a `PLAYED_IN` relationship type and a `role` property, we would use the following interpretation:

```yaml
- type: relationship
  node_type: Movie
  relationship_type: PLAYED_IN
  node_key:
    title: !jmespath movie_title
  relationship_key:
    role: !jmespath role
```

This would allow us to have multiple `PLAYED_IN` relationships between the same source node and related node as long as they have different `role` properties.

### Relationship Match Strategy

By default, related nodes are created when not present based on the supplied node type and key. However, you can change this behavior by setting the `match_strategy` field. The `match_strategy` field is a string that can be one of the following values:

* `EAGER` - Related nodes will be created when not present based on the supplied node type and key. (Default)
* `MATCH_ONLY` - Will not create a relationship when the related node does not already exists.
* `FUZZY` - Behaves like `MATCH ONLY`, but treats the key values as regular expressions to match on.

### Working with Many Relationships

By default, the cardinality of a relationship is one-to-one (or many; depending on the data). 
This means that the relationship interpretation will only relate to one node per instance of the interpretation. 
Oftentimes when working with document data, you might encounter a list of values all of which should be related to the source node. 
To accomplish this, you can use the `find_many` field. 
The `find_many` field is a boolean that represents whether or not the searches provided to `node_key` can return multiple values, and thus should create multiple relationships to multiple related nodes.

For example, if we wanted to define a `HAS_CHILD` relationship between a source node of type `Person` and a related node of type `Person` with the `HAS_CHILD` relationship type, we would use the following interpretation:

```yaml
- type: relationship
  node_type: Person
  relationship_type: HAS_CHILD
  node_key:
    name: !jmespath children[*].name
  find_many: true
```

Alternatively, you can use the `iterate_on` clause to iterate over a list of values and create a relationship for each one.
When using `iterate_on`, the `find_many` field is not required and will be ignored.
Additionally, the `node_key` and `node_properties` fields are addressed relative to the current object in the iteration.

For example, the same interpretation as above could be written like this:

```yaml
- type: relationship
  node_type: Person
  relationship_type: HAS_CHILD
  iterate_on: !jmespath children[*]
  node_key:
    name: !jmespath name
```


| Parameter Name          	| Required? 	| Type                               	| Description                                                                                                                                                                                                                                                                                                            	|
|-------------------------	|-----------	|------------------------------------	|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| node_type               	| Yes       	| String or ValueProvider            	| Specifies the type of the node a relationship connects to. It is a required field. When a ValueProvider is used dynamic index creation and schema introspection are not supported.                                                                                                                                    	|
| relationship_type       	| Yes       	| String or ValueProvider            	| Specifies the type of the relationship. It is a required field. When a ValueProvider is used dynamic index creation and schema introspection are not supported.                                                                                                                                                        	|
| node_key                	| Yes       	| Dictionary                         	| Contains key-value pairs that define the key of the related node. The keys represent field names, and the values can be either static values or value providers. It is a required field.                                                                                                                               	|
| node_properties         	| No        	| Dictionary                         	| Stores additional properties of the related node. It is a dictionary where the keys represent property names, and the values can be either static values or value providers. This field is optional.                                                                                                                   	|
| relationship_key        	| No        	| Dictionary                         	| Contains key-value pairs that define the key of the relationship itself. The keys represent field names, and the values can be either static values or value providers. It is a required field. The uniqueness of the relationship is defined in terms of the nodes it is relating and the key of the relationship.    	|
| relationship_properties 	| No        	| Dictionary                         	| Stores additional properties of the relationship It is a dictionary where the keys represent property names, and the values can be either static values or value providers. This field is optional.                                                                                                                    	|
| outbound                	| No        	| Boolean                            	| Represents whether or not the relationship direction is outbound from the source node. By default, this is true.                                                                                                                                                                                                       	|
| find_many               	| No        	| Boolean                            	| Represents whether or not the searches provided to node_key can return multiple values, and thus should create multiple relationships to multiple related nodes.                                                                                                                                                       	|
| iterate_on              	| No        	| ValueProvider                      	| Iterates over the values provided by the supplied value provider, and creates an relationship for each one.                                                                                                                                                                                                            	|
| match_strategy          	| No        	| `EAGER` \| `MATCH_ONLY` \| `FUZZY` 	| Defaults to `EAGER`. When `EAGER`, related nodes will be created when not present based on the supplied node type and key. `MATCH_ONLY` will not create a relationship when the related node does not already exists. `FUZZY` behaves like `MATCH ONLY`, but treats the key values as regular expressions to match on. 	|
| key_normalization       	| No        	| Dictionary                         	| Contains normalization flags that should be adopted by value providers when getting values for node and relationship keys. This field is optional. [See the normalization reference](./normalizers.md).  By default  `do_lowercase_strings` is enabled.                                                                                    	|
| property_normalization  	| No        	| Dictionary                         	| Contains normalization flags that should be adopted by value providers when getting values for node and relationship properties. This field is optional. [See the normalization reference](./normalizers.md). By default no flags are enabled.                                                                                             	|

## Properties Interpretation

The properties interpretation is used to define additional properties on the source node. 
The properties interpretation uses a dictionary where the keys represent property names, and the values can be either static values or [value providers](./value-providers.md).

For example, if we wanted to define a `birthday` property and a `meaning_of_life` property on a source node, we would use the following interpretation:

```yaml
- type: properties
  properties:
    birthday: !jmespath birthday
    meaning_of_life: 42
```

| Parameter Name 	| Required? 	| Type       	| Description                                                                                                                                                                                                	|
|----------------	|-----------	|------------	|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| properties     	| Yes       	| Dictionary 	| Stores additional properties of the source node. It is a dictionary where the keys represent property names, and the values can be either static values or value providers. This field is optional.        	|
| normalization  	| No        	| Dictionary 	| Contains normalization flags that should be adopted by value providers when getting values. This field is optional. [See the normalization reference](./normalizers.md).  By default no flags are enabled. 	|

## Variables Interpretation

The variables interpretation is used to define variables that can be referenced later with the [`!variable`](./value-providers.md#variable) value provider. For example, if we wanted to define a variable called `meaning_of_life` with a value of `42`, we would use the following interpretation:

```yaml
- type: variables
  variables:
    meaning_of_life: 42
```

And it could be referenced later like this:

```yaml
- type: properties
  properties:
    meaning_of_life: !variable meaning_of_life
```

NOTE: that variables can be defined either statically or using a value provider. 

| Parameter Name 	| Required? 	| Type       	| Description                                                                                                                                                                                                	|
|----------------	|-----------	|------------	|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| variables     	| Yes       	| Dictionary 	| Stores values as variables that can be referenced later with the [`!variable`](./value-providers.md#variable) value provider. It is a dictionary where the keys represent property names, and the values can be either static values or value providers. This field is optional.        	|
| normalization  	| No        	| Dictionary 	| Contains normalization flags that should be adopted by value providers when getting values. This field is optional. [See the normalization reference](./normalizers.md).  By default no flags are enabled. 	|


## Switch Interpretation

The switch interpretation allows you to define multiple interpretations that will be applied. The interpretation that will be applied is determined by the value of the `switch_on` parameter. The value of the `switch_on` parameter is a value provider that will be evaluated for each source node. The value of the value provider will be used to determine which interpretation to apply. The interpretation that will be applied is the one that has the same value as the value of the `switch_on` parameter. If no interpretation has the same value as the value of the `switch_on` parameter, the default interpretation will be applied. If no default interpretation is defined, the source node will be ignored.

As an example, if we wanted to define a switch interpretation that would apply a `Person` interpretation if the `type` field of the source node was `person`, and a `Company` interpretation if the `type` field of the source node was `company`, we would use the following interpretation:

```yaml
- type: switch
  switch_on: !jmespath type
  interpretations:
    person:
      - type: source_node
        node_type: Person
        key:
          name: !jmespath name
    company:
      - type: source_node
        node_type: Company
        key:
          name: !jmespath name
```


| Parameter Name 	| Required? 	| Type       	| Description                                                                                                                                                                                                	|
|----------------	|-----------	|------------	|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| switch_on     	| Yes       	| ValueProvider 	| The value provider that will be evaluated for each source node. The value of the value provider will be used to determine which interpretation to apply.        	|
| interpretations  	| Yes        	| Dictionary 	| Contains the interpretations that will be applied. The keys represent the values of the `switch_on` parameter. The values represent the interpretations that will be applied. Each value may also be a list of interpretations.	|
| default  	| No        	| Dictionary 	| Contains the default interpretation that will be applied if no interpretation has the same value as the value of the `switch_on` parameter. 	|
