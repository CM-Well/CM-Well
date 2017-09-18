let { Link } = ReactRouter

class Type extends React.Component {
    constructor(props) {
        super(props)
        
        this.state = { }
    }
    
    render() {
        AppUtils.debug('Type.render')
        let href = this.props.uri || location.pathname // todo combine with qp
        let count = this.props.count ? ` (${this.props.count.toLocaleString()})` : ''
        let label = (this.props.label || AppUtils.lastPartOfUrl(this.props.uri)) + count
        return <span className="type">{ this.props.selected ? label : <Link to={href}>{label}</Link> }</span>
    }
}

class ExpandButton extends React.Component {
    constructor(props) {
        super(props)
        
        this.state = { }
    }

    clickCb() {
        this.props.clickCb && this.props.clickCb()
//        this.toggleState('expanded')
    }
    
    render() {
        let className = 'expand-button' + (this.props.isExpanded ? ' expanded' : '')
        return <img className={className} width="25" height="25" src="/meta/app/main/images/blue-down-arrow.svg" onClick={this.clickCb.bind(this)} />
    }
}

class Types extends React.Component {
    constructor(props) {
        super(props)
        
        this.state = {
            types: []
        }
        
        this.separator = <span className="separator">|</span>
        this.all = <Type label="All" selected={true} />
    }
        
    componentWillReceiveProps() {
        AppUtils.cachedGet(`${this.props.location.pathname}?op=aggregate&recursive&ap=type:term,field::type.rdf,size:1024&format=json`).then(resp => {
            resp.AggregationResponse && this.setState({ types: resp.AggregationResponse[0].buckets.map(bckt => { return { uri: bckt.key, count: bckt.objects } }) })
        })
        this.setState({ expanded: false })
    }
    
    render() {
        AppUtils.debug('Types.render')
        let className = `types-container ${this.state.expanded ? 'expanded' : ''}`
        let types = [this.all, ...this.state.types.map(t => <Type uri={t.uri} count={t.count} />)]
                                                       
        let isExpandButtonNeeded = this.state.types.length > 5 // todo is hard-coding 5 is the best we can do here?
                                                       
        return <div className={className}>
            <div className="types">View by: { AppUtils.addSep(types, this.separator) }</div>
           { isExpandButtonNeeded ? <ExpandButton isExpanded={this.state.expanded} clickCb={this.toggleState.bind(this, 'expanded')}/> : null }
        </div>
    }
}

define([], () => Types)