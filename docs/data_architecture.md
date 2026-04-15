## 📊 Data Architecture
```
    [ SOURCES ]                                      [ DATA WAREHOUSE ]                                     [ CONSUME ]



 ┌────────────┐                     ┌──────────────┐   ┌──────────────┐   ┌──────────────┐                ┌──────────────┐
 │   ERP      │                     │   BRONZE     │   │    SILVER    │   │     GOLD     │                │   Power BI   │
 │  (XLSX)    │                     │──────────────│   │──────────────│   │──────────────│                └──────────────┘
 └────────────┘                     │ • Raw Data   │   │ • Clean Data │   │ • Business   │                ┌──────────────┐
                                    │ • Table      │   │ • Table      │   │   Data       │                │ SQL Queries │
                     ─────────▶    │ • Batch Load │   │ • Batch Load │   │ • KPIs       │    ─────────▶  └──────────────┘
                                    │ • Full Load  │   │ • Full Load  │   │ • Metrics    │                ┌──────────────┐
                                    │ • No Transf. │   │ • Cleaning   │   │ • Aggregates │                │ Machine Learn│
                                    │ • Model: None│   │ • Normalize  │   │ • Fact/Dim   │                └──────────────┘
                                    │              │   │ • Standardize│   │ • Star Schema│
                                    │              │   │ • Derived Col│   │              │
                                    │              │   │ • Enrichment │   │              │
                                    │              │   │ • Model: None│   │              │
                                    └──────────────┘   └──────────────┘   └──────────────┘
```



<mxfile host="app.diagrams.net">
  <diagram name="Data Architecture">
    <mxGraphModel dx="1200" dy="800" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1">
      <root>
        <mxCell id="0"/>
        <mxCell id="1" parent="0"/>

        <!-- SOURCES -->
        <mxCell id="crm" value="CRM" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#ffffff;" vertex="1" parent="1">
          <mxGeometry x="40" y="120" width="100" height="50" as="geometry"/>
        </mxCell>

        <mxCell id="erp" value="ERP (XLSX)" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#ffffff;" vertex="1" parent="1">
          <mxGeometry x="40" y="200" width="100" height="50" as="geometry"/>
        </mxCell>

        <!-- BRONZE -->
        <mxCell id="bronze" value="BRONZE LAYER&#xa;----------------------&#xa;• Raw Data&#xa;• Object: Table&#xa;• Load: Batch Processing&#xa;• Full Load (Truncate & Insert)&#xa;• No Transformation&#xa;• Data Model: None"
        style="rounded=1;whiteSpace=wrap;html=1;strokeColor=#ff9999;" vertex="1" parent="1">
          <mxGeometry x="200" y="100" width="220" height="220" as="geometry"/>
        </mxCell>

        <!-- SILVER -->
        <mxCell id="silver" value="SILVER LAYER&#xa;----------------------&#xa;• Clean Standard Data&#xa;• Object: Table&#xa;• Load: Batch Processing&#xa;• Full Load (Truncate & Insert)&#xa;• Data Cleaning&#xa;• Data Normalization&#xa;• Data Standardization&#xa;• Derived Columns&#xa;• Data Enrichment&#xa;• Data Model: None (As-Is)"
        style="rounded=1;whiteSpace=wrap;html=1;strokeColor=#cccccc;" vertex="1" parent="1">
          <mxGeometry x="450" y="100" width="240" height="260" as="geometry"/>
        </mxCell>

        <!-- GOLD -->
        <mxCell id="gold" value="GOLD LAYER&#xa;----------------------&#xa;• Business Ready Data&#xa;• KPIs & Metrics&#xa;• Aggregations&#xa;• Fact & Dimension Tables&#xa;• Optimized for Reporting&#xa;• Data Model: Star Schema"
        style="rounded=1;whiteSpace=wrap;html=1;strokeColor=#ffcc00;" vertex="1" parent="1">
          <mxGeometry x="720" y="100" width="220" height="220" as="geometry"/>
        </mxCell>

        <!-- CONSUME -->
        <mxCell id="consume" value="CONSUME&#xa;----------------------&#xa;• Power BI&#xa;• SQL Queries&#xa;• Machine Learning"
        style="rounded=1;whiteSpace=wrap;html=1;fillColor=#ffffff;" vertex="1" parent="1">
          <mxGeometry x="980" y="140" width="180" height="150" as="geometry"/>
        </mxCell>

        <!-- ARROWS -->
        <mxCell id="e1" edge="1" parent="1" source="erp" target="bronze" style="endArrow=block;">
          <mxGeometry relative="1" as="geometry"/>
        </mxCell>

        <mxCell id="e2" edge="1" parent="1" source="bronze" target="silver" style="endArrow=block;">
          <mxGeometry relative="1" as="geometry"/>
        </mxCell>

        <mxCell id="e3" edge="1" parent="1" source="silver" target="gold" style="endArrow=block;">
          <mxGeometry relative="1" as="geometry"/>
        </mxCell>

        <mxCell id="e4" edge="1" parent="1" source="gold" target="consume" style="endArrow=block;">
          <mxGeometry relative="1" as="geometry"/>
        </mxCell>

      </root>
    </mxGraphModel>
  </diagram>
</mxfile>
