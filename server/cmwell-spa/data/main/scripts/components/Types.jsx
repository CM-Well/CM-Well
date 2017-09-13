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

class Types extends React.Component {
    constructor(props) {
        super(props)
        
        this.state = {
            types: []
        }
        
        this.separator = <span className="separator">|</span>
        this.all = <Type label="All" selected={true} />
    }
        
    componentWillMount() {
        console.log('Types will receive props')
        AppUtils.cachedGet(`${this.props.location.pathname}?op=aggregate&recursive&ap=type:term,field::type.rdf,size:1024&format=json`).then(resp => {
            resp.AggregationResponse && this.setState({ types: resp.AggregationResponse[0].buckets.map(bckt => { return { uri: bckt.key, count: bckt.objects } }) })
        })
    }
    
    render() {
        AppUtils.debug('Types.render')
        let types = [this.all, ...this.state.types.map(t => <Type uri={t.uri} count={t.count} />)]
        return <div className="types-container">View by: { AppUtils.addSep(types, this.separator) }</div>
    }
}

define([], () => Types)