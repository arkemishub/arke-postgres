# Copyright 2023 Arkemis S.r.l.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

defmodule ArkePostgres.Query do
  import Ecto.Query
  alias Arke.DatetimeHandler, as: DatetimeHandler

  @record_fields [:id, :arke_id, :data, :metadata, :inserted_at, :updated_at]

  def generate_query(
        %{filters: filters, orders: orders, offset: offset, limit: limit} = arke_query,
        action
      ) do
    base_query(arke_query, action)
    |> handle_filters(filters)
    |> handle_orders(orders)
    |> handle_offset(offset)
    |> handle_limit(limit)
  end

  def execute(query, :raw),
    do: Ecto.Adapters.SQL.to_sql(:all, ArkePostgres.Repo, generate_query(query, :raw))

  def execute(query, :all) do
    generate_query(query, :all)
    |> ArkePostgres.Repo.all(prefix: query.project)
    |> generate_units(query.arke, query.project)
  end

  def execute(query, :one) do
    record = generate_query(query, :one) |> ArkePostgres.Repo.one(prefix: query.project)
    init_unit(record, query.arke, query.project)
  end

  def execute(query, :count) do
    generate_query(query, :count) |> ArkePostgres.Repo.one(prefix: query.project)
  end

  def execute(query, :pseudo_query), do: generate_query(query, :pseudo_query)

  def get_column(%{data: %{persistence: "arke_parameter"}} = parameter),
    do: get_arke_column(parameter)

  def get_column(%{data: %{persistence: "table_column"}} = parameter),
    do: get_table_column(parameter)

  def get_manager_units(project_id) do
    arke_link = Arke.Boundary.ArkeManager.get(:arke_link, :arke_system)

    links =
      from(q in table_query(arke_link, nil), where: q.type in ["parameter", "group"])
      |> ArkePostgres.Repo.all(prefix: project_id)

    parameter_links = Enum.filter(links, fn x -> x.type == "parameter" end)
    group_links = Enum.filter(links, fn x -> x.type == "group" end)

    parameters_id = [
      "boolean",
      "dict",
      "list",
      "float",
      "integer",
      "string",
      "unit",
      "link",
      "dynamic",
      "date",
      "datetime",
      "time"
    ]

    list_arke_id = parameters_id ++ ["arke", "group"]

    unit_list =
      from(q in base_query(), where: q.arke_id in ^list_arke_id)
      |> ArkePostgres.Repo.all(prefix: project_id)
      |> generate_units(nil, project_id)

    parameters = Enum.filter(unit_list, fn u -> Atom.to_string(u.arke_id) in parameters_id end)

    arke_list =
      parse_arke_list(
        Enum.filter(unit_list, fn u -> Atom.to_string(u.arke_id) == "arke" end),
        parameter_links
      )

    groups =
      parse_groups(
        Enum.filter(unit_list, fn u -> Atom.to_string(u.arke_id) == "group" end),
        group_links
      )

    {parameters, arke_list, groups}
  end

  defp parse_arke_list(arke_list, parameter_links) do
    Enum.reduce(arke_list, [], fn %{id: id} = unit, new_arke_list ->
      params =
        Enum.reduce(
          Enum.filter(parameter_links, fn x -> x.parent_id == Atom.to_string(id) end),
          [],
          fn p, new_params ->
            [%{id: String.to_existing_atom(p.child_id), metadata: p.metadata} | new_params]
          end
        )

      [Arke.Core.Unit.update(unit, parameters: params) | new_arke_list]
    end)
  end

  defp parse_groups(groups, group_links) do
    Enum.reduce(groups, [], fn %{id: id} = unit, new_groups ->
      arke_list =
        Enum.reduce(
          Enum.filter(group_links, fn x -> x.parent_id == Atom.to_string(id) end),
          [],
          fn p, new_params ->
            [%{id: String.to_existing_atom(p.child_id), metadata: p.metadata} | new_params]
          end
        )

      [Arke.Core.Unit.update(unit, arke_list: arke_list) | new_groups]
    end)
  end

  #  def get_parameters(project_id) do
  #    query =  base_query()
  #    parameters_id = ["boolean", "dict", "float", "integer", "string", "unit", "link", "date", "datetime", "time"]
  #    (from q in query, where: q.arke_id in ^parameters_id)
  #                            |>  ArkePostgres.Repo.all(prefix: project_id) |> generate_units(nil, project_id)
  #  end
  #
  #  def get_arke_list(project_id) do
  #    arke_list = (from q in base_query(), where: q.arke_id == "arke")
  #      |>  ArkePostgres.Repo.all(prefix: project_id) |> generate_units("arke", project_id)
  #
  #    arke_link = Arke.Boundary.ArkeManager.get(:arke_link, :arke_system)
  #    links = (from q in table_query(arke_link, nil), where: q.type in ["parameter", "group"])
  #      |>  ArkePostgres.Repo.all(prefix: project_id)
  #    parameter_links = Enum.filter(parameter_links, fn x -> x.type == "parameter" end)
  #    group_links = Enum.filter(parameter_links, fn x -> x.type == "group" end)
  #
  #    Enum.reduce(arke_list, [], fn %{id: id}=unit, new_arke_list ->
  #      params = Enum.reduce(Enum.filter(parameter_links, fn x -> x.parent_id == Atom.to_string(id) end), [], fn p, new_params ->
  #        [%{id: String.to_existing_atom(p.child_id), type: nil, metadata: p.metadata} | new_params]
  #      end)
  #      [Arke.Core.Unit.update(unit, parameters: params) | new_arke_list]
  #    end)
  #  end

  ######################################################################################################################
  # PRIVATE FUNCTIONS ##################################################################################################
  ######################################################################################################################
  defp base_query(%{arke: %{data: %{type: "table"}, id: id} = arke} = _arke_query, action),
    do: table_query(arke, action)

  defp base_query(%{link: nil} = _arke_query, action), do: arke_query(action)

  defp base_query(%{link: link, project: project} = _arke_query, action),
    do:
      get_nodes(
        project,
        action,
        Atom.to_string(link.unit.id),
        link.depth,
        link.direction,
        link.type
      )

  defp base_query(), do: from("arke_unit", select: ^@record_fields)

  defp arke_query(:count), do: from("arke_unit", select: count("*"))
  defp arke_query(_action), do: from("arke_unit", select: ^@record_fields)

  defp table_query(%{id: id, data: data} = arke, action) do
    table_name = Atom.to_string(id)

    fields =
      Enum.reduce(data.parameters, [], fn %{id: parameter_id}, new_fields ->
        [parameter_id | new_fields]
      end)

    from(table_name, select: ^fields)
  end

  defp get_arke(project, %{arke_id: arke_id}, nil) when is_binary(arke_id),
    do: Arke.Boundary.ArkeManager.get(String.to_existing_atom(arke_id), project)

  defp get_arke(project, %{arke_id: arke_id}, nil) when is_atom(arke_id),
    do: Arke.Boundary.ArkeManager.get(arke_id, project)

  defp get_arke(project, %{arke_id: arke_id}, nil),
    do: Arke.Boundary.ArkeManager.get(String.to_existing_atom(arke_id), project)

  defp get_arke(project, %{arke_id: arke_id}, arke) when is_atom(arke),
    do: Arke.Boundary.ArkeManager.get(arke, project)

  defp get_arke(project, %{arke_id: arke_id}, arke) when is_binary(arke),
    do: Arke.Boundary.ArkeManager.get(String.to_existing_atom(arke), project)

  defp get_arke(_, %{arke_id: arke_id}, arke), do: arke
  defp get_arke(_, _data, arke), do: arke

  defp generate_units(data, arke, project) do
    Enum.reduce(data, [], fn d, units ->
      units ++ [init_unit(d, arke, project)]
    end)
  end

  defp init_unit(nil, _, _), do: nil

  defp init_unit(record, arke, project) do
    arke = get_arke(project, record, arke)
    {metadata, record} = Map.pop(record, :metadata)
    {record_data, record} = Map.pop(record, :data, %{})
    record_data =
      Enum.map(arke.data.parameters, fn p ->
        {p.id, Map.get(record_data, Atom.to_string(p.id), nil)}
      end)
      |> Map.new()

    #    record_data = Enum.map(record_data, fn {k, v} -> {String.to_existing_atom(k), v} end) |> Map.new()
    record = Map.put(record, :metadata, Map.merge(metadata, %{project: project}))
    record = Map.merge(record_data, record)
    Arke.Core.Unit.load(arke, record)
  end

  defp handle_filters(query, filters) do
    Enum.reduce(filters, query, fn %{logic: logic, negate: negate, base_filters: base_filters},
                                   new_query ->
      clause = handle_condition(logic, base_filters) |> handle_negate_condition(negate)
      from(q in new_query, where: ^clause)
    end)
  end

  defp handle_condition(logic, base_filters) do
    Enum.reduce(base_filters, nil, fn %{
                                        parameter: parameter,
                                        operator: operator,
                                        value: value,
                                        negate: negate
                                      },
                                      clause ->
      column = get_column(parameter)
      value = get_value(parameter, value)

      if is_nil(value) or operator == :isnull do
        condition = get_nil_query(parameter, column)
        add_condition_to_clause(condition, clause, logic)
      else
        condition =
          filter_query_by_operator(column, value, operator) |> handle_negate_condition(negate)

        add_condition_to_clause(condition, clause, logic)
      end
    end)
  end

  defp handle_negate_condition(condition, true), do: dynamic([q], not (^condition))
  defp handle_negate_condition(condition, false), do: condition

  defp add_condition_to_clause(condition, nil, _), do: dynamic([q], ^condition)
  defp add_condition_to_clause(condition, clause, :and), do: dynamic([q], ^clause and ^condition)
  defp add_condition_to_clause(condition, clause, :or), do: dynamic([q], ^clause or ^condition)

  defp handle_orders(query, orders) do
    order_by =
      Enum.reduce(orders, [], fn %{parameter: parameter, direction: direction}, new_order_by ->
        column = get_column(parameter)
        [{direction, column} | new_order_by]
      end)

    from(q in query, order_by: ^order_by)
  end

  defp handle_offset(query, offset) when is_nil(offset), do: query
  defp handle_offset(query, offset), do: from(q in query, offset: ^offset)

  defp handle_limit(query, limit) when is_nil(limit), do: query
  defp handle_limit(query, limit), do: from(q in query, limit: ^limit)

  defp get_table_column(%{id: id} = _parameter), do: dynamic([q], fragment("?", field(q, ^id)))

  defp get_arke_column(%{id: id, arke_id: :string} = _parameter),
    do: dynamic([q], fragment("(? -> ? ->> 'value')::text", field(q, :data), ^Atom.to_string(id)))

  defp get_arke_column(%{id: id, arke_id: :atom} = _parameter),
    do: dynamic([q], fragment("(? -> ? ->> 'value')::text", field(q, :data), ^Atom.to_string(id)))

  defp get_arke_column(%{id: id, arke_id: :boolean} = _parameter),
    do:
      dynamic(
        [q],
        fragment("(? -> ? ->> 'value')::boolean", field(q, :data), ^Atom.to_string(id))
      )

  defp get_arke_column(%{id: id, arke_id: :datetime} = _parameter),
    do:
      dynamic(
        [q],
        fragment("(? -> ? ->> 'value')::datetime", field(q, :data), ^Atom.to_string(id))
      )

  defp get_arke_column(%{id: id, arke_id: :date} = _parameter),
    do:
      dynamic(
        [q],
        fragment("(? -> ? ->> 'value')::date", field(q, :data), ^Atom.to_string(id))
      )

  defp get_arke_column(%{id: id, arke_id: :time} = _parameter),
    do: dynamic([q], fragment("(? -> ? ->> 'value')::time", field(q, :data), ^Atom.to_string(id)))

  defp get_arke_column(%{id: id, arke_id: :integer} = _parameter),
    do:
      dynamic(
        [q],
        fragment("(? -> ? ->> 'value')::integer", field(q, :data), ^Atom.to_string(id))
      )

  defp get_arke_column(%{id: id, arke_id: :float} = _parameter),
    do:
      dynamic([q], fragment("(? -> ? ->> 'value')::float", field(q, :data), ^Atom.to_string(id)))

  defp get_arke_column(%{id: id, arke_id: :dict} = _parameter),
    do: dynamic([q], fragment("(? -> ? ->> 'value')::JSON", field(q, :data), ^Atom.to_string(id)))

  defp get_arke_column(%{id: id, arke_id: :list} = _parameter),
    do: dynamic([q], fragment("(? -> ? ->> 'value')::JSON", field(q, :data), ^Atom.to_string(id)))

  defp get_arke_column(%{id: id, arke_id: :link} = _parameter),
    do: dynamic([q], fragment("(? -> ? ->> 'value')::text", field(q, :data), ^Atom.to_string(id)))

    defp get_arke_column(%{id: id, arke_id: :dynamic} = _parameter),
    do: dynamic([q], fragment("(? -> ? ->> 'value')::text", field(q, :data), ^Atom.to_string(id)))

  defp get_value(_parameter, value) when is_nil(value), do: value
  defp get_value(_parameter, value) when is_list(value), do: value
  defp get_value(_parameter, value) when is_map(value), do: value

  defp get_value(parameter, value) when is_atom(value) and not is_boolean(value),
    do: get_value(parameter, Atom.to_string(value))

  defp get_value(%{id: id, arke_id: :string} = _parameter, value) when is_binary(value), do: value
  defp get_value(%{id: id, arke_id: :string} = _parameter, value), do: Kernel.inspect(value)

  defp get_value(%{id: id, arke_id: :integer} = _parameter, value) when is_number(value),
    do: value

  defp get_value(%{id: id, arke_id: :integer} = _parameter, value) when is_binary(value) do
    case Integer.parse(value) do
      {value, _remainder} -> value
      _ -> raise("Parameter(#{id}) value not valid")
    end
  end

  defp get_value(%{id: id, arke_id: :integer} = parameter, value) when is_list(value),
    do: parse_number_list(parameter, value, fn v -> is_integer(v) end)

  defp get_value(%{id: id, arke_id: :float} = _parameter, value) when is_number(value), do: value

  defp get_value(%{id: id, arke_id: :float} = _parameter, value) when is_binary(value) do
    case Float.parse(value) do
      {value, _remainder} -> value
      _ -> raise("Parameter(#{id}) value not valid")
    end
  end

  defp get_value(%{id: id, arke_id: :float} = parameter, value) when is_list(value),
    do: parse_number_list(parameter, value, fn v -> is_number(v) end)

  defp get_value(%{id: id, arke_id: :boolean} = _parameter, true), do: true
  defp get_value(%{id: id, arke_id: :boolean} = _parameter, "true"), do: true
  defp get_value(%{id: id, arke_id: :boolean} = _parameter, "True"), do: true
  defp get_value(%{id: id, arke_id: :boolean} = _parameter, 1), do: true
  defp get_value(%{id: id, arke_id: :boolean} = _parameter, "1"), do: true
  defp get_value(%{id: id, arke_id: :boolean} = _parameter, false), do: false
  defp get_value(%{id: id, arke_id: :boolean} = _parameter, "false"), do: false
  defp get_value(%{id: id, arke_id: :boolean} = _parameter, "False"), do: false
  defp get_value(%{id: id, arke_id: :boolean} = _parameter, 0), do: false
  defp get_value(%{id: id, arke_id: :boolean} = _parameter, "0"), do: false

  defp get_value(%{id: id, arke_id: :boolean} = _parameter, _),
    do: raise("Parameter(#{id}) value not valid")

  defp get_value(%{id: id, arke_id: :datetime} = _parameter, value) do
    case DatetimeHandler.parse_datetime(value) do
      {:ok, parsed_datetime} -> parsed_datetime
      {:error, _msg} -> raise("Parameter(#{id}) value not valid")
    end
  end

  defp get_value(%{id: id, arke_id: :date} = _parameter, value) do
    case DatetimeHandler.parse_date(value) do
      {:ok, parsed_date} -> parsed_date
      {:error, _msg} -> raise("Parameter(#{id}) value not valid")
    end
  end

  defp get_value(%{id: id, arke_id: :time} = _parameter, value) do
    case DatetimeHandler.parse_time(value) do
      {:ok, parsed_time} -> parsed_time
      {:error, _msg} -> raise("Parameter(#{id}) value not valid")
    end
  end

  defp get_value(%{id: id, arke_id: :dict} = _parameter, value), do: value
  defp get_value(%{id: id, arke_id: :list} = _parameter, value), do: value
  defp get_value(%{id: id, arke_id: :link} = _parameter, value), do: value
  defp get_value(%{id: id, arke_id: :dynamic} = _parameter, value), do: value
  defp get_value(%{id: id}, value), do: raise("Parameter(#{id}) value not valid")

  defp parse_number_list(parameter, value, func) do
    case Enum.all?(value, &is_binary(&1)) do
      true ->
        Enum.map(value, &get_value(parameter, &1))

      false ->
        # check if all the values are numbers, otherwise throw an error
        case Enum.all?(value, &func.(&1)) do
          true -> Enum.map(value, &get_value(parameter, &1))
          false -> raise("Parameter(#{parameter.id}) value not valid")
        end
    end
  end

  defp get_nil_query(%{id: id} = _parameter, column),
    do:
      dynamic(
        [q],
        fragment("? IS NULL AND (data \\? ?)", ^column, ^Atom.to_string(id))
      )

  defp filter_query_by_operator(column, value, :eq), do: dynamic([q], ^column == ^value)

  defp filter_query_by_operator(column, value, :contains),
    do: dynamic([q], like(^column, fragment("?", ^("%" <> value <> "%"))))

  defp filter_query_by_operator(column, value, :icontains),
    do: dynamic([q], ilike(^column, fragment("?", ^("%" <> value <> "%"))))

  defp filter_query_by_operator(column, value, :endswith),
    do: dynamic([q], like(^column, fragment("?", ^("%" <> value))))

  defp filter_query_by_operator(column, value, :iendswith),
    do: dynamic([q], ilike(^column, fragment("?", ^("%" <> value))))

  defp filter_query_by_operator(column, value, :startswith),
    do: dynamic([q], like(^column, fragment("?", ^(value <> "%"))))

  defp filter_query_by_operator(column, value, :istartswith),
    do: dynamic([q], ilike(^column, fragment("?", ^(value <> "%"))))

  defp filter_query_by_operator(column, value, :lte), do: dynamic([q], ^column <= ^value)
  defp filter_query_by_operator(column, value, :lt), do: dynamic([q], ^column < ^value)
  defp filter_query_by_operator(column, value, :gt), do: dynamic([q], ^column > ^value)
  defp filter_query_by_operator(column, value, :gte), do: dynamic([q], ^column >= ^value)
  defp filter_query_by_operator(column, value, :in), do: dynamic([q], ^column in ^value)
  defp filter_query_by_operator(column, value, _), do: dynamic([q], ^column == ^value)

  # defp filter_query_by_operator(query, key, value, "between"), do: from q in query, where: column_table(q, ^key) == ^value

  ######################################################################################################################
  # ARKE LINK ##########################################################################################################
  ######################################################################################################################

  @raw_cte_child_query """
  (
    WITH RECURSIVE tree(depth, parent_id, type, child_id, metadata) AS (
      SELECT 0, parent_id, type, child_id, metadata FROM ?.arke_link WHERE ? = ?
      UNION SELECT
        depth + 1,
        ?.arke_link.parent_id,
        ?.arke_link.type,
        ?.arke_link.child_id,
        ?.arke_link.metadata
      FROM
        ?.arke_link JOIN tree
        ON ?.arke_link.? = tree.?
      WHERE
       depth < ?
    )
    SELECT * FROM tree ORDER BY depth
  )
  """
  @raw_cte_parent_query """
  (
    WITH RECURSIVE tree(depth, parent_id, type, child_id, metadata) AS (
      SELECT 0, parent_id, type, child_id, metadata FROM arke_link WHERE child_id = ?
      UNION SELECT
        depth + 1,
        arke_link.parent_id,
        arke_link.type,
        arke_link.child_id,
        arke_link.metadata
      FROM
        arke_link JOIN tree
        ON arke_link.child_id = tree.parent_id
      WHERE
       depth < ?
    )
    SELECT * FROM tree ORDER BY depth
  )
  """

  #  def get_nodes(project, :count, unit_id, depth, direction) do
  #      from a in "arke_unit",
  #           left_join: cte in fragment(@raw_cte_child_query, literal(^Atom.to_string(project)), literal(^"parent_id"), ^unit_id, literal(^"parent_id"), literal(^"child_id"), ^depth),
  #           where: a.id == cte.child_id,
  #           select: count("*")
  #  end
  def get_nodes(project, action, unit_id, depth, direction, type) do
    project = get_project(project)
    {link_field, tree_field} = get_fields_by_direction(direction)
    where_field = get_where_field_by_direction(direction) |> get_where_condition_by_type(type)
    get_link_query(action, project, unit_id, link_field, tree_field, depth, where_field)
  end

  defp get_project(project) when is_atom(project), do: Atom.to_string(project)
  defp get_project(project), do: project
  defp get_fields_by_direction(:child), do: {"parent_id", "child_id"}
  defp get_fields_by_direction(:parent), do: {"child_id", "parent_id"}

  defp get_where_field_by_direction(:child),
    do: dynamic([a, cte], a.id == fragment("?", field(cte, ^:child_id)))

  defp get_where_field_by_direction(:parent),
    do: dynamic([a, cte], a.id == fragment("?", field(cte, ^:parent_id)))

  defp get_where_condition_by_type(condition, nil), do: condition

  defp get_where_condition_by_type(condition, type),
    do: dynamic([a, cte], ^condition and cte.type == ^type)

  defp get_where_field_by_direction(:parent),
    do: dynamic([a, cte], a.id == fragment("?", field(cte, ^:parent_id)))

  defp get_link_query(:count, project, unit_id, link_field, tree_field, depth, where_field) do
    from(a in "arke_unit",
      left_join:
        cte in fragment(
          @raw_cte_child_query,
          literal(^project),
          literal(^link_field),
          ^unit_id,
          literal(^project),
          literal(^project),
          literal(^project),
          literal(^project),
          literal(^project),
          literal(^project),
          literal(^link_field),
          literal(^tree_field),
          ^depth
        ),
      where: ^where_field,
      select: count("*")
    )
  end

  defp get_link_query(_action, project, unit_id, link_field, tree_field, depth, where_field) do
    from(a in "arke_unit",
      left_join:
        cte in fragment(
          @raw_cte_child_query,
          literal(^project),
          literal(^link_field),
          ^unit_id,
          literal(^project),
          literal(^project),
          literal(^project),
          literal(^project),
          literal(^project),
          literal(^project),
          literal(^link_field),
          literal(^tree_field),
          ^depth
        ),
      where: ^where_field,
      select: %{
        id: a.id,
        arke_id: a.arke_id,
        data: a.data,
        metadata: a.metadata,
        inserted_at: a.inserted_at,
        updated_at: a.updated_at,
        depth: cte.depth,
        link_metadata: cte.metadata,
        link_type: cte.type
      }
    )
  end

  #  defp get_select_by_action(_action), do: dynamic([a, cte], merge(map(a, [:id, :arke_id, :data, :metadata, :inserted_at, :updated_at]), map(cte, [:depth, :metadata, :type])))
end
