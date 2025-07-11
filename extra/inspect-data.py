import streamlit as st
import pandas as pd
import plotly.express as px

# App Title
st.title("CSV Scatterplot Viewer")

# File Upload
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

# Process the uploaded file
if uploaded_file is not None:
    # Load the CSV file into a DataFrame
    df = pd.read_csv(uploaded_file)

    st.write("Original data preview:")
    # Display the dataframe with sorting enabled
    sorted_df = st.data_editor(
        df,
        use_container_width=True,
        num_rows="dynamic",
    )

    # Dropdowns for selecting x and y axes
    x_axis = st.selectbox("Select X-axis column", sorted_df.columns, index=0)
    y_axis = st.selectbox("Select Y-axis column", sorted_df.columns, index=1)

    # Sorting options
    sort_by = st.multiselect(
        "Sort by column(s)",
        sorted_df.columns,
        help="Select columns to sort the data."
    )
    ascending_order = st.checkbox("Sort in ascending order", value=True)

    if sort_by:
        sorted_df = sorted_df.sort_values(by=sort_by, ascending=ascending_order)

    # Display sorted data preview
    st.write("Sorted Data Preview:")
    st.dataframe(sorted_df)

    # Generate scatterplot
    if x_axis and y_axis:
        fig = px.scatter(sorted_df, x=x_axis, y=y_axis, title=f"Scatterplot: {x_axis} vs {y_axis}")
        st.plotly_chart(fig)