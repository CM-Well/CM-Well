class ActionsBar extends React.Component {
    constructor(props) {
        super(props)
    }
    
    getFormat(fmt, selectBox) {
        let fields = this.props.isFiltering ? `&fields=${this.props.fields.join`,`}` : ''
        fmt && window.open(`${location.pathname}?format=${fmt}&override-mimetype=text/plain${fields}`)
        selectBox.selectedIndex = 0
        selectBox.blur()
    }

    render() {
        let formatDropDown = this.props.forInfoton ? <span>
                View in format: 
                <select onChange={e=>this.getFormat(e.target.value, e.target)}>
                    <option value="" key="0">Select format</option>
                    {AppUtils.formats.map(fmt => <option value={fmt.value} key={fmt.value}>{fmt.displayName}</option>)}
                </select>        
            </span> : null


        let className = `actions-bar${this.props.forInfoton ? " for-infoton" : (this.props.forInfotonsList ? " for-infotons-list" : "")}`
        
        return <div className={className}>
            {formatDropDown}
        </div>
    }

}

define([], () => ActionsBar)